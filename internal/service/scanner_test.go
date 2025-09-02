package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/cerisilv123/ethereum-monitor/internal/models"
)

// ---- Mocks ----

type mockEthereumClient struct {
	fetchFunc func(number *big.Int, fullTx bool) (*types.Block, error)
}

func (m *mockEthereumClient) FetchBlock(ctx context.Context, number *big.Int, fullTx bool) (*types.Block, error) {
	return m.fetchFunc(number, fullTx)
}

func (m *mockEthereumClient) LatestBlockNumber(ctx context.Context) (uint64, error) { return 0, nil }
func (m *mockEthereumClient) TxReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	return nil, nil
}
func (m *mockEthereumClient) WatchNewHeads(ctx context.Context, ch chan<- *types.Header) (Subscription, error) {
	return nil, nil
}
func (m *mockEthereumClient) Close() error { return nil }

type mockPublisher struct {
	calls []models.TxMatchedEvent
	err   error
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, key, val []byte) error {
	if m.err != nil {
		return m.err
	}
	var ev models.TxMatchedEvent
	_ = json.Unmarshal(val, &ev)
	m.calls = append(m.calls, ev)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

// ---- Helpers ----

func makeSignedTx(t *testing.T, chainID *big.Int, to *common.Address, value *big.Int) (*types.Transaction, common.Address) {
	key, _ := crypto.GenerateKey()
	fromAddr := crypto.PubkeyToAddress(key.PublicKey)

	var toVal common.Address
	if to != nil {
		toVal = *to
	}

	tx := types.NewTransaction(0, toVal, value, 21000, big.NewInt(1), nil)

	signed, err := types.SignTx(tx, types.NewEIP155Signer(chainID), key)
	if err != nil {
		t.Fatalf("failed to sign tx: %v", err)
	}
	return signed, fromAddr
}

func makeBlock(num uint64, txs []*types.Transaction) *types.Block {
	hdr := &types.Header{
		Number: new(big.Int).SetUint64(num),
		Time:   uint64(time.Now().Unix()),
	}

	blk := types.NewBlockWithHeader(hdr)

	body := types.Body{
		Transactions: txs,
		Uncles:       []*types.Header{},
		Withdrawals:  nil,
	}

	return blk.WithBody(body)
}

// ---- Tests ----

func TestProcessBlock_FetchErrorRecordsFailure(t *testing.T) {
	// Arrange
	client := &mockEthereumClient{
		fetchFunc: func(number *big.Int, fullTx bool) (*types.Block, error) {
			return nil, errors.New("boom")
		},
	}
	pub := &mockPublisher{}
	scanner := &Scanner{
		logger:     slog.Default(),
		client:     client,
		publisher:  pub,
		matchTopic: "topic",
	}

	// Act
	ctx := context.Background()
	scanner.processBlock(ctx, big.NewInt(100))

	// Assert
	if len(scanner.failedBlockNums) != 1 || scanner.failedBlockNums[0] != 100 {
		t.Errorf("expected block 100 recorded as failed, got %+v", scanner.failedBlockNums)
	}
	if len(pub.calls) != 0 {
		t.Errorf("expected no publish calls, got %d", len(pub.calls))
	}
}

func TestProcessBlock_MatchFromAndTo(t *testing.T) {
	// Arrange
	toKey, _ := crypto.GenerateKey()
	toAddr := crypto.PubkeyToAddress(toKey.PublicKey)

	tx, fromAddr := makeSignedTx(t, big.NewInt(1), &toAddr, big.NewInt(1e18))
	block := makeBlock(50, []*types.Transaction{tx})

	client := &mockEthereumClient{
		fetchFunc: func(n *big.Int, _ bool) (*types.Block, error) {
			return block, nil
		},
	}
	pub := &mockPublisher{}
	scanner := &Scanner{
		logger:     slog.Default(),
		client:     client,
		publisher:  pub,
		matchTopic: "topic",
		addresses: map[string]string{
			strings.ToLower(fromAddr.Hex()): "userFrom",
			strings.ToLower(toAddr.Hex()):   "userTo",
		},
	}

	// Act
	ctx := context.Background()
	scanner.processBlock(ctx, big.NewInt(50))

	// Assert
	if len(pub.calls) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(pub.calls))
	}
	expectedUsers := map[string]bool{"userFrom": true, "userTo": true}
	for _, ev := range pub.calls {
		if !expectedUsers[ev.UserID] {
			t.Errorf("unexpected userID %s in event", ev.UserID)
		}
		if ev.BlockNumber != 50 {
			t.Errorf("wrong block number: %d", ev.BlockNumber)
		}
	}
}

func TestProcessBlock_PublisherErrorRecorded(t *testing.T) {
	// Arrange
	toKey, _ := crypto.GenerateKey()
	toAddr := crypto.PubkeyToAddress(toKey.PublicKey)

	tx, fromAddr := makeSignedTx(t, big.NewInt(1), &toAddr, big.NewInt(5))
	block := makeBlock(77, []*types.Transaction{tx})

	client := &mockEthereumClient{
		fetchFunc: func(n *big.Int, _ bool) (*types.Block, error) { return block, nil },
	}
	pub := &mockPublisher{err: errors.New("kafka down")}
	scanner := &Scanner{
		logger:     slog.Default(),
		client:     client,
		publisher:  pub,
		matchTopic: "topic",
		addresses: map[string]string{
			strings.ToLower(fromAddr.Hex()): "userX",
		},
	}

	// Act
	ctx := context.Background()
	scanner.processBlock(ctx, big.NewInt(77))

	// Assert
	if len(scanner.failedEvents) != 1 {
		t.Fatalf("expected 1 failed event recorded, got %d", len(scanner.failedEvents))
	}
	if scanner.failedEvents[0].UserID != "userX" {
		t.Errorf("wrong userID in failed event: %+v", scanner.failedEvents[0])
	}
}

func TestRecordFailedBlock_AppendsBlockNumber(t *testing.T) {
	// Arrange
	scanner := &Scanner{logger: slog.Default()}

	// Act
	scanner.recordFailedBlock(123)

	// Assert
	if len(scanner.failedBlockNums) != 1 || scanner.failedBlockNums[0] != 123 {
		t.Errorf("expected failedBlockNums to contain 123, got %+v", scanner.failedBlockNums)
	}
}

func TestRecordFailedEvent_AppendsEvent(t *testing.T) {
	// Arrange
	ev := models.TxMatchedEvent{UserID: "user1", Hash: "0xdead", BlockNumber: 42}
	scanner := &Scanner{logger: slog.Default()}

	// Act
	scanner.recordFailedEvent(ev)

	// Assert
	if len(scanner.failedEvents) != 1 || scanner.failedEvents[0].UserID != "user1" {
		t.Errorf("expected failedEvents to contain %+v, got %+v", ev, scanner.failedEvents)
	}
}

func TestRetryFailedBlocks_ProcessesAgain(t *testing.T) {
	// Arrange
	called := false
	client := &mockEthereumClient{
		fetchFunc: func(n *big.Int, _ bool) (*types.Block, error) {
			called = true
			// return empty block
			return makeBlock(n.Uint64(), nil), nil
		},
	}
	scanner := &Scanner{
		logger:     slog.Default(),
		client:     client,
		rpcTimeout: time.Second,
	}
	scanner.failedBlockNums = []uint64{99}

	// Act
	scanner.retryFailedBlocks(context.Background())

	// Assert
	if called != true {
		t.Errorf("expected FetchBlock to be called for failed block")
	}
	if len(scanner.failedBlockNums) != 0 {
		t.Errorf("expected failedBlockNums to be cleared, got %+v", scanner.failedBlockNums)
	}
}

func TestRetryFailedEvents_RePublishesEvents(t *testing.T) {
	// Arrange
	pub := &mockPublisher{}
	scanner := &Scanner{
		logger:     slog.Default(),
		publisher:  pub,
		matchTopic: "topic",
	}
	ev := models.TxMatchedEvent{UserID: "u1", From: "0xabc", To: "0xdef", Amount: "1.23", Hash: "0xdead", BlockNumber: 7}
	scanner.failedEvents = []models.TxMatchedEvent{ev}

	// Act
	scanner.retryFailedEvents(context.Background())

	// Assert
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	if pub.calls[0].UserID != "u1" {
		t.Errorf("expected userID u1, got %s", pub.calls[0].UserID)
	}
	if len(scanner.failedEvents) != 0 {
		t.Errorf("expected failedEvents to be cleared, got %+v", scanner.failedEvents)
	}
}

func TestHandleMatch_PublishesEvent(t *testing.T) {
	// Arrange
	pub := &mockPublisher{}
	scanner := &Scanner{
		logger:     slog.Default(),
		publisher:  pub,
		matchTopic: "topic",
	}

	// Act
	ctx := context.Background()
	scanner.handleMatch(ctx, "user123", "0xfrom", "0xto", "42", "0xhash", 123, "FROM")

	// Assert
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(pub.calls))
	}
	ev := pub.calls[0]
	if ev.UserID != "user123" || ev.BlockNumber != 123 {
		t.Errorf("unexpected event: %+v", ev)
	}
}
