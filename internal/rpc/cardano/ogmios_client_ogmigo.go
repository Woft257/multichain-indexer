package cardano

import (
	"context"
	"sync"

	ogmigo "github.com/SundaeSwap-finance/ogmigo/v6"
)

// realOgmiosClient implements OgmiosAPI using github.com/SundaeSwap-finance/ogmigo
// NOTE: For first phase we keep parsing minimal and use the stream as a liveness
// signal and future cache feeder. Blocks sent on the channel will currently be
// nil until parsers are added; the Indexer falls back to REST for full data.
// This still gives us a live WS connection and room to extend without affecting
// other chains.
type realOgmiosClient struct {
	endpoint string
	headers  map[string]string

	mu     sync.Mutex
	cli    *ogmigo.Client
	start  sync.Once
	bch    chan *Block
	ech    chan error
	closeC chan struct{}
}

func newRealOgmiosClient(url string, headers map[string]string) OgmiosAPI {
	return &realOgmiosClient{
		endpoint: url,
		headers:  headers,
		bch:      make(chan *Block, 64),
		ech:      make(chan error, 1),
		closeC:   make(chan struct{}),
	}
}

func (c *realOgmiosClient) ensureClient() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cli != nil {
		return nil
	}
	// Build client with endpoint; headers are not wired at this moment because
	// ogmigo options may differ across versions. Demeter typically requires an
	// API key header; if unsupported by the transport, supply it via reverse proxy
	// or configure endpoint accordingly.
	c.cli = ogmigo.New(ogmigo.WithEndpoint(c.endpoint))
	return nil
}

func (c *realOgmiosClient) Start(ctx context.Context) (<-chan *Block, <-chan error) {
	c.start.Do(func() {
		if err := c.ensureClient(); err != nil {
			select { case c.ech <- err: default: }
			close(c.bch)
			close(c.ech)
			return
		}

		go func() {
			// Basic chain-sync loop using ogmigo. The callback receives raw bytes.
			// For now, we treat it as a heartbeat; parsing can be added later.
			var cb ogmigo.ChainSyncFunc = func(ctx context.Context, data []byte) error {
				// TODO: decode "data" into a Block header and push to c.bch when parser is ready.
				// To keep the stream active and non-blocking we no-op here.
				return nil
			}

			closer, err := c.cli.ChainSync(ctx, cb)
			if err != nil {
				select { case c.ech <- err: default: }
				close(c.bch)
				close(c.ech)
				return
			}
			// Wait for context cancellation
			<-ctx.Done()
			_ = closer.Close()
			close(c.bch)
			close(c.ech)
		}()
	})
	return c.bch, c.ech
}

func (c *realOgmiosClient) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	// For now, return 0 so the Indexer falls back to REST for tip; future work: use ogmigo state-query.
	return 0, nil
}

func (c *realOgmiosClient) Close() error {
	// Best-effort: the ChainSync closer is handled in Start when ctx is done.
	return nil
}

