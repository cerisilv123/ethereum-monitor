package ethereum

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/cerisilv123/ethereum-monitor/internal/service"
)

var _ service.EthereumClient = (*MultiRPCClient)(nil)

// MultiRPCClient wraps multiple RPCClients with failover + retry.
type MultiRPCClient struct {
	mu         sync.RWMutex
	clients    []*RPCClient
	current    int
	logger     *slog.Logger
	maxRetries int
	baseDelay  time.Duration
}

// NewMultiRPCClient tries to connect to all given URLs, skipping any that fail.
// Each client will be retried `maxRetries` times before failing over.
func NewMultiRPCClient(urls []string, logger *slog.Logger, maxRetries int, baseDelay time.Duration) (*MultiRPCClient, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("no RPC URLs provided")
	}

	var clients []*RPCClient
	for _, u := range urls {
		c, err := ConnectRPC(u)
		if err != nil {
			logger.Warn("Failed to connect to RPC endpoint", "url", u, "error", err)
			continue
		}
		logger.Info("Connected to RPC endpoint", "url", u)
		clients = append(clients, c)
	}

	if len(clients) == 0 {
		return nil, fmt.Errorf("failed to connect to any RPC endpoints")
	}

	if maxRetries < 1 {
		maxRetries = 1
	}

	if baseDelay <= 0 {
		baseDelay = 100 * time.Millisecond
	}

	return &MultiRPCClient{
		clients:    clients,
		logger:     logger.With("component", "MultiRPCClient"),
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
	}, nil
}

// getClient returns the current client.
func (m *MultiRPCClient) getClient() *RPCClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.clients[m.current]
}

// failover moves to the next available client.
func (m *MultiRPCClient) failover() {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.current
	m.current = (m.current + 1) % len(m.clients)
	m.logger.Warn("Failing over to next RPC endpoint",
		"from", old,
		"to", m.current,
	)
}

// --- Implement EthereumClient methods with retry + failover ---

func (m *MultiRPCClient) FetchBlock(ctx context.Context, number *big.Int, fullTx bool) (*types.Block, error) {
	for i := 0; i < len(m.clients); i++ {
		client := m.getClient()

		var block *types.Block
		err := m.withRetry(ctx, func() error {
			b, err := client.FetchBlock(ctx, number, fullTx)
			if err != nil {
				m.logger.Error("FetchBlock failed",
					"client", m.current,
					"error", err,
				)
				return err
			}
			block = b
			return nil
		})

		if err == nil {
			return block, nil
		}
		m.failover()
	}

	return nil, fmt.Errorf("all RPC endpoints failed fetching block %v", number)
}

func (m *MultiRPCClient) LatestBlockNumber(ctx context.Context) (uint64, error) {
	for i := 0; i < len(m.clients); i++ {
		client := m.getClient()

		var num uint64
		err := m.withRetry(ctx, func() error {
			n, err := client.LatestBlockNumber(ctx)
			if err != nil {
				m.logger.Error("LatestBlockNumber failed",
					"client", m.current,
					"error", err,
				)
				return err
			}
			num = n
			return nil
		})

		if err == nil {
			return num, nil
		}
		m.failover()
	}
	return 0, fmt.Errorf("all RPC endpoints failed fetching latest block")
}

func (m *MultiRPCClient) TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	for i := 0; i < len(m.clients); i++ {
		client := m.getClient()

		var receipt *types.Receipt
		err := m.withRetry(ctx, func() error {
			r, err := client.TxReceipt(ctx, hash)
			if err != nil {
				m.logger.Error("TxReceipt failed",
					"client", m.current,
					"tx", hash.Hex(),
					"error", err,
				)
				return err
			}
			receipt = r
			return nil
		})

		if err == nil {
			return receipt, nil
		}
		m.failover()
	}
	return nil, fmt.Errorf("all RPC endpoints failed fetching receipt for %s", hash.Hex())
}

func (m *MultiRPCClient) WatchNewHeads(ctx context.Context, ch chan<- *types.Header) (service.Subscription, error) {
	for i := 0; i < len(m.clients); i++ {
		client := m.getClient()

		var sub service.Subscription
		err := m.withRetry(ctx, func() error {
			s, err := client.WatchNewHeads(ctx, ch)
			if err != nil {
				m.logger.Error("WatchNewHeads failed",
					"client", m.current,
					"error", err,
				)
				return err
			}
			sub = s
			return nil
		})

		if err == nil {
			return sub, nil
		}
		m.failover()
	}
	return nil, fmt.Errorf("all RPC endpoints failed subscribing to new heads")
}

func (m *MultiRPCClient) Close() error {
	for _, c := range m.clients {
		_ = c.Close()
	}
	return nil
}

// withRetry retries fn up to maxRetries with exponential backoff.
func (m *MultiRPCClient) withRetry(ctx context.Context, fn func() error) error {
	for attempt := 1; attempt <= m.maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		// If this was the last attempt, return the error
		if attempt == m.maxRetries {
			return fmt.Errorf("max retries exceeded: %w", err)
		}

		// Exponential backoff: 100ms, 200ms, 400ms, ...
		backoff := time.Duration(math.Pow(2, float64(attempt-1))) * m.baseDelay
		m.logger.Warn("Retrying after failure",
			"attempt", attempt,
			"maxRetries", m.maxRetries,
			"backoff", backoff,
			"err", err,
		)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
