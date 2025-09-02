package service

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Ethereum client
type EthereumClient interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
	FetchBlock(ctx context.Context, num *big.Int, fullTx bool) (*types.Block, error)
	TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	WatchNewHeads(ctx context.Context, ch chan<- *types.Header) (Subscription, error)
	Close() error
}

type Subscription interface {
	Unsubscribe()
	Err() <-chan error
}

// Kafka
type EventPublisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}
