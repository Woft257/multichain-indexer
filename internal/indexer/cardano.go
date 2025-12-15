package indexer

import (
	"context"
	"fmt"
	"strings"
	"time"
	"sync"


	"github.com/fystack/multichain-indexer/internal/rpc"
	"github.com/fystack/multichain-indexer/internal/rpc/cardano"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/shopspring/decimal"
)


type CardanoIndexer struct {
	chainName   string
	config      config.ChainConfig
	failover    *rpc.Failover[cardano.CardanoAPI]
	pubkeyStore PubkeyStore // optional: for selective tx fetching

	// Optional Ogmios streaming for real-time blocks
	ogmios     cardano.OgmiosAPI
	cacheMu    sync.RWMutex
	cache      map[uint64]*types.Block
	cacheOrder []uint64
	cacheCap   int
}


// StartOgmios begins consuming blocks from Ogmios and caching them for fast access.
// Safe to call multiple times; subsequent calls are no-ops.
func (c *CardanoIndexer) StartOgmios(ctx context.Context) {
	if c.ogmios == nil {
		return
	}
	// initialize cache once
	c.cacheMu.Lock()
	if c.cache == nil {
		c.cache = make(map[uint64]*types.Block, 256)
	}
	if c.cacheCap == 0 {
		c.cacheCap = 200
	}
	c.cacheMu.Unlock()

	bch, _ := c.ogmios.Start(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case b, ok := <-bch:
				if !ok || b == nil {
					return
				}
				// convert and cache
				tb := c.convertBlock(b)
				c.cachePut(tb)
			}
		}
	}()
}

func (c *CardanoIndexer) cachePut(b *types.Block) {
	if b == nil {
		return
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cache == nil {
		c.cache = make(map[uint64]*types.Block, 256)
	}
	if _, exists := c.cache[b.Number]; !exists {
		c.cacheOrder = append(c.cacheOrder, b.Number)
		if c.cacheCap > 0 && len(c.cacheOrder) > c.cacheCap {
			old := c.cacheOrder[0]
			c.cacheOrder = c.cacheOrder[1:]
			delete(c.cache, old)
		}
	}
	c.cache[b.Number] = b
}

func (c *CardanoIndexer) cacheGet(num uint64) (*types.Block, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	if c.cache == nil {
		return nil, false
	}
	b, ok := c.cache[num]
	return b, ok
}

func NewCardanoIndexer(
	chainName string,
	cfg config.ChainConfig,
	failover *rpc.Failover[cardano.CardanoAPI],
	pubkeyStore PubkeyStore,
	ogmios cardano.OgmiosAPI,
) *CardanoIndexer {
	ci := &CardanoIndexer{
		chainName:   chainName,
		config:      cfg,
		failover:    failover,
		pubkeyStore: pubkeyStore,
		ogmios:      ogmios,
		cacheCap:    200,
		cache:       make(map[uint64]*types.Block, 256),
	}
	return ci
}

func (c *CardanoIndexer) GetName() string                  { return strings.ToUpper(c.chainName) }
func (c *CardanoIndexer) GetNetworkType() enum.NetworkType { return enum.NetworkTypeCardano }
func (c *CardanoIndexer) GetNetworkInternalCode() string {
	return c.config.InternalCode
}
func (c *CardanoIndexer) GetNetworkId() string {
	return c.config.NetworkId
}

// GetLatestBlockNumber fetches the latest block number
func (c *CardanoIndexer) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	var latest uint64
	err := c.failover.ExecuteWithRetry(ctx, func(api cardano.CardanoAPI) error {
		n, err := api.GetLatestBlockNumber(ctx)
		latest = n
		return err
	})
	return latest, err
}

// GetBlock fetches a single block (header + txs fetched in parallel with quota)
func (c *CardanoIndexer) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	var (
		header   *cardano.BlockResponse
		txHashes []string
		txs      []cardano.Transaction
	)

	err := c.failover.ExecuteWithRetry(ctx, func(api cardano.CardanoAPI) error {
		var err error
		header, err = api.GetBlockHeaderByNumber(ctx, blockNumber)
		if err != nil {
			return err
		}
		// Use header.Hash to avoid extra header lookup in tx listing
		txHashes, err = api.GetTransactionsByBlockHash(ctx, header.Hash)
		if err != nil {
			return err
		}
		concurrency := c.config.Throttle.Concurrency
		if concurrency <= 0 {
			concurrency = cardano.DefaultTxFetchConcurrency
		}
		// Clamp concurrency to the number of transactions to avoid creating useless goroutines
		if numTxs := len(txHashes); numTxs > 0 && numTxs < concurrency {
			concurrency = numTxs
		}
		// If pubkeyStore is available, do selective fetching to reduce quota usage
		if c.pubkeyStore != nil {
			checker := func(addr string) bool { return c.pubkeyStore.Exist(enum.NetworkTypeCardano, addr) }
			txs, err = api.FetchTransactionsSelective(ctx, txHashes, concurrency, checker)
			return err
		}
		txs, err = api.FetchTransactionsParallel(ctx, txHashes, concurrency)
		return err
	})
	if err != nil {
		return nil, err
	}

	block := &cardano.Block{
		Hash:       header.Hash,
		Height:     header.Height,
		Slot:       header.Slot,
		Time:       header.Time,
		ParentHash: header.ParentHash,
	}
	// attach txs
	for i := range txs {
		block.Txs = append(block.Txs, txs[i])
	}

	return c.convertBlock(block), nil
}

// GetBlocks fetches a range of blocks (hybrid: prefer Ogmios cache near head)
func (c *CardanoIndexer) GetBlocks(
	ctx context.Context,
	from, to uint64,
	isParallel bool,
) ([]BlockResult, error) {
	if to < from {
		return nil, fmt.Errorf("invalid range: from=%d, to=%d", from, to)
	}

	blockNums := make([]uint64, 0, to-from+1)
	for n := from; n <= to; n++ {
		blockNums = append(blockNums, n)
	}

	return c.fetchBlocks(ctx, blockNums, isParallel)
}

// GetBlocksByNumbers fetches blocks by their numbers
func (c *CardanoIndexer) GetBlocksByNumbers(
	ctx context.Context,
	blockNumbers []uint64,
) ([]BlockResult, error) {
	return c.fetchBlocks(ctx, blockNumbers, false)
}

// fetchBlocks is the internal method to fetch blocks
func (c *CardanoIndexer) fetchBlocks(
	ctx context.Context,
	blockNums []uint64,
	isParallel bool,
) ([]BlockResult, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	workers := len(blockNums)
	if c.config.Throttle.Concurrency > 0 && c.config.Throttle.Concurrency < workers {
		workers = c.config.Throttle.Concurrency
	} else if c.config.Throttle.Concurrency <= 0 && workers > cardano.DefaultTxFetchConcurrency {
		workers = cardano.DefaultTxFetchConcurrency
	}

	type job struct {
		num   uint64
		index int
	}

	jobs := make(chan job, len(blockNums))
	results := make([]BlockResult, len(blockNums))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Early exit if context is canceled
				select {
				case <-ctx.Done():
					return
				default:
				}

				block, err := c.GetBlock(ctx, j.num)
				if err != nil {
					logger.Warn("failed to fetch block", "block", j.num, "error", err)
					results[j.index] = BlockResult{
						Number: j.num,
						Error:  &Error{ErrorType: ErrorTypeBlockNotFound, Message: err.Error()},
					}
				} else {
					results[j.index] = BlockResult{Number: j.num, Block: block}
				}
			}
		}()
	}

	// Feed jobs to workers and close channel when done
	go func() {
		defer close(jobs)
		for i, num := range blockNums {
			select {
			case jobs <- job{num: num, index: i}:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	// Check if the context was canceled during the operation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return results, nil
}

// convertBlock converts a Cardano block to the common Block type
func (c *CardanoIndexer) convertBlock(block *cardano.Block) *types.Block {
	// Pre-allocate slice with a reasonable capacity to reduce re-allocations
	estimatedSize := len(block.Txs) * 2
	transactions := make([]types.Transaction, 0, estimatedSize)

	for _, tx := range block.Txs {
		// Skip failed transactions (e.g., script validation failed)
		// valid when: no script (nil) OR smart contract executed successfully (true)
		if tx.ValidContract != nil && !*tx.ValidContract {
			continue
		}
		// Find a representative from address from non-reference, non-collateral inputs
		fromAddr := ""
		for _, inp := range tx.Inputs {
			if !inp.Reference && !inp.Collateral && inp.Address != "" {
				fromAddr = inp.Address
				break
			}
		}

		// Convert fee (lovelace -> ADA) and assign to the first transfer produced by this tx
		feeAda := decimal.NewFromInt(int64(tx.Fee)).Div(decimal.NewFromInt(1_000_000))
		feeAssigned := false

		for _, out := range tx.Outputs {
			// Skip collateral outputs as they are not considered transfers to the recipient
			if out.Collateral {
				continue
			}
			for _, amt := range out.Amounts {
				if amt.Quantity == "" || amt.Quantity == "0" {
					continue
				}
				tr := types.Transaction{
					TxHash:      tx.Hash,
					NetworkId:   c.GetNetworkId(),
					BlockNumber: block.Height,
					FromAddress: fromAddr,
					ToAddress:   out.Address,
					Amount:      amt.Quantity,
					Type:        constant.TxnTypeTransfer,
					Timestamp:   block.Time,
				}
				if amt.Unit != "lovelace" {
					tr.AssetAddress = amt.Unit
				}
				if !feeAssigned {
					tr.TxFee = feeAda
					feeAssigned = true
				}
				transactions = append(transactions, tr)
			}
		}
	}

	return &types.Block{
		Number:       block.Height,
		Hash:         block.Hash,
		ParentHash:   block.ParentHash,
		Timestamp:    block.Time,
		Transactions: transactions,
	}
}

// IsHealthy checks if the indexer is healthy
func (c *CardanoIndexer) IsHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.GetLatestBlockNumber(ctx)
	return err == nil
}

