package cardano

import (
	"context"

	"github.com/fystack/multichain-indexer/internal/rpc"
)

// CardanoAPI defines the interface for Cardano RPC operations
type CardanoAPI interface {
	rpc.NetworkClient
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	GetBlockHeaderByNumber(ctx context.Context, blockNumber uint64) (*BlockResponse, error)
	GetBlockByNumber(ctx context.Context, blockNumber uint64) (*Block, error)
	GetBlockHash(ctx context.Context, blockNumber uint64) (string, error)
	GetTransactionsByBlock(ctx context.Context, blockNumber uint64) ([]string, error)
	// New: fetch txs by block hash to avoid an extra header call when we already have the hash
	GetTransactionsByBlockHash(ctx context.Context, blockHash string) ([]string, error)
	GetTransaction(ctx context.Context, txHash string) (*Transaction, error)
	// New: get only UTXOs of a tx for prefiltering
	GetTransactionUTxOs(ctx context.Context, txHash string) (*TxUTxOsResponse, error)
	FetchTransactionsParallel(ctx context.Context, txHashes []string, concurrency int) ([]Transaction, error)
	// New: selective fetching using UTXOs to prefilter by monitored addresses
	FetchTransactionsSelective(ctx context.Context, txHashes []string, concurrency int, addressChecker func(addr string) bool) ([]Transaction, error)
	GetBlockByHash(ctx context.Context, blockHash string) (*Block, error)
}

