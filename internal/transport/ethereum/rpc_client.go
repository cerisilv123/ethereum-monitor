package ethereum

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/event" // needed for Subscription wrapper

	"github.com/cerisilv123/ethereum-monitor/internal/service"
)

var _ service.EthereumClient = (*RPCClient)(nil)
var _ service.Subscription = (*subscriptionWrapper)(nil)

type RPCClient struct {
	raw *ethclient.Client
}

func ConnectRPC(rpcURL string) (*RPCClient, error) {
	c, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC %q: %w", rpcURL, err)
	}
	return &RPCClient{raw: c}, nil
}

func (c *RPCClient) FetchBlock(ctx context.Context, number *big.Int, fullTx bool) (*types.Block, error) {
	block, err := c.raw.BlockByNumber(ctx, number)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block %v: %w", number, err)
	}
	return block, nil
}

func (c *RPCClient) LatestBlockNumber(ctx context.Context) (uint64, error) {
	num, err := c.raw.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block number: %w", err)
	}
	return num, nil
}

func (c *RPCClient) TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	r, err := c.raw.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get receipt for tx %s: %w", hash.Hex(), err)
	}
	return r, nil
}

func (c *RPCClient) WatchNewHeads(ctx context.Context, ch chan<- *types.Header) (service.Subscription, error) {
	sub, err := c.raw.SubscribeNewHead(ctx, ch)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	return &subscriptionWrapper{sub}, nil
}

func (c *RPCClient) Close() error {
	c.raw.Close()
	return nil
}

// --- Wrapper to satisfy service.Subscription ---

type subscriptionWrapper struct {
	event.Subscription
}

func (s *subscriptionWrapper) Unsubscribe() {
	s.Subscription.Unsubscribe()
}

func (s *subscriptionWrapper) Err() <-chan error {
	return s.Subscription.Err()
}
