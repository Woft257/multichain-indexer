package cardano

import (
	"context"
	"strings"
)

// OgmiosAPI is a minimal interface we use from the indexer to
// optionally stream Cardano blocks over a websocket connection.
// Implemented by a real client in ogmios_client_ogmigo.go
// (github.com/SundaeSwap-finance/ogmigo).
type OgmiosAPI interface {
	// Start begins the streaming loop. It returns a read-only channel of blocks
	// and a read-only channel of fatal errors. Implementations should close
	// both channels on exit. Calling Start multiple times should be safe and
	// return the same channels.
	Start(ctx context.Context) (<-chan *Block, <-chan error)
	// GetLatestBlockNumber returns the current chain tip height if available.
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	// Close shuts down the underlying connection.
	Close() error
}

// NewCardanoOgmiosClient creates an OgmiosAPI from a URL and optional headers.
// The real implementation is provided in ogmios_client_ogmigo.go.
func NewCardanoOgmiosClient(url string, headers map[string]string) OgmiosAPI {
	return newRealOgmiosClient(url, headers)
}

// Helper to detect ogmios scheme quickly by URL prefix.
func IsOgmiosURL(u string) bool {
	us := strings.ToLower(strings.TrimSpace(u))
	return strings.HasPrefix(us, "ws://") || strings.HasPrefix(us, "wss://")
}

