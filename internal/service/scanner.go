package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	"github.com/cerisilv123/ethereum-monitor/internal/models"
)

type Scanner struct {
	logger            *slog.Logger
	client            EthereumClient
	addresses         map[string]string
	workerThreads     int
	bufferSize        int
	publisher         EventPublisher
	matchTopic        string
	rpcTimeout        time.Duration
	retryInterval     time.Duration
	lastEnqueuedBlock atomic.Uint64

	failedBlocksMu  sync.Mutex
	failedBlockNums []uint64

	failedEventsMu sync.Mutex
	failedEvents   []models.TxMatchedEvent
}

type ScannerConfig struct {
	Logger        *slog.Logger
	Client        EthereumClient
	Addresses     map[string]string
	WorkerThreads int
	BufferSize    int
	Publisher     EventPublisher
	MatchTopic    string
	RPCTimeout    time.Duration
	RetryInterval time.Duration
}

func NewScanner(cfg ScannerConfig) *Scanner {
	normedAddrs := make(map[string]string, len(cfg.Addresses))
	for userID, addr := range cfg.Addresses {
		normedAddrs[strings.ToLower(addr)] = userID
	}

	return &Scanner{
		logger:        cfg.Logger,
		client:        cfg.Client,
		addresses:     normedAddrs,
		workerThreads: cfg.WorkerThreads,
		bufferSize:    cfg.BufferSize,
		publisher:     cfg.Publisher,
		matchTopic:    cfg.MatchTopic,
		rpcTimeout:    cfg.RPCTimeout,
		retryInterval: cfg.RetryInterval,
	}
}

// Run starts the scanner loop and workers.
func (s *Scanner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	headers := make(chan *types.Header, s.bufferSize)
	sub, err := s.client.WatchNewHeads(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer sub.Unsubscribe()

	s.logger.Info("Scanner started",
		"workers", s.workerThreads,
		"bufferSize", s.bufferSize,
	)

	work := make(chan *types.Header, s.bufferSize)

	var wg sync.WaitGroup
	for i := 0; i < s.workerThreads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for h := range work {
				s.logger.Debug("Worker picked up block", "workerID", id, "block", h.Number.Uint64())
				s.processBlock(ctx, h.Number)
			}
			s.logger.Info("Worker stopped", "id", id)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.startFailedBlockRetry(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.startFailedEventRetry(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			close(work)
			cancel()
			wg.Wait()
			return nil
		case err, ok := <-sub.Err():
			close(work)
			cancel()
			wg.Wait()
			if !ok || err == nil {
				return fmt.Errorf("subscription closed")
			}
			return fmt.Errorf("subscription error: %v", err)
		case header := <-headers:
			n := header.Number.Uint64()
			last := s.lastEnqueuedBlock.Load()
			if n <= last {
				// duplicate or older header, already enqueued
				continue
			}
			s.lastEnqueuedBlock.Store(n)
			work <- header
		}
	}
}

func (s *Scanner) startFailedBlockRetry(ctx context.Context) {
	ticker := time.NewTicker(s.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping failed block retry worker")
			return
		case <-ticker.C:
			s.retryFailedBlocks(ctx)
		}
	}
}

func (s *Scanner) startFailedEventRetry(ctx context.Context) {
	ticker := time.NewTicker(s.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping failed event retry worker")
			return
		case <-ticker.C:
			s.retryFailedEvents(ctx)
		}
	}
}

func (s *Scanner) retryFailedEvents(ctx context.Context) {
	s.failedEventsMu.Lock()
	failed := make([]models.TxMatchedEvent, len(s.failedEvents))
	copy(failed, s.failedEvents)
	s.failedEvents = nil // reset
	s.failedEventsMu.Unlock()

	if len(failed) == 0 {
		return
	}

	s.logger.Info("Retrying failed publish events", "count", len(failed))

	for _, ev := range failed {
		s.logger.Debug("Retrying event publish", "user", ev.UserID, "tx", ev.Hash, "block", ev.BlockNumber)
		s.publishMatch(ctx, ev.UserID, ev.From, ev.To, ev.Amount, ev.Hash, ev.BlockNumber)
	}
}

// retryFailedBlocks drains the failedBlockNums list and attempts to reprocess each one.
func (s *Scanner) retryFailedBlocks(ctx context.Context) {
	s.failedBlocksMu.Lock()
	failed := make([]uint64, len(s.failedBlockNums))
	copy(failed, s.failedBlockNums)
	s.failedBlockNums = nil // reset
	s.failedBlocksMu.Unlock()

	if len(failed) == 0 {
		return
	}

	s.logger.Info("Retrying failed blocks", "count", len(failed), "blocks", failed)

	for _, blockNum := range failed {
		num := big.NewInt(int64(blockNum))
		s.logger.Debug("Retrying failed block", "block", blockNum)
		s.processBlock(ctx, num)
	}
}

// processBlock fetches and processes a block by number.
// Retries fetching with exponential backoff and records failures for retry later.
func (s *Scanner) processBlock(ctx context.Context, num *big.Int) {
	s.logger.Debug("Fetching block", "block", num.String())

	blockCtx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()

	block, err := s.client.FetchBlock(blockCtx, num, true)
	if err != nil {
		s.logger.Error("Error fetching block", "block", num.Uint64(), "err", err)
		s.recordFailedBlock(num.Uint64())
		return
	}

	s.logger.Info("Processing block", "block", block.NumberU64(), "txCount", len(block.Transactions()))

	for _, tx := range block.Transactions() {
		var signer types.Signer
		if tx.ChainId() == nil || tx.ChainId().Cmp(big.NewInt(0)) == 0 {
			signer = types.HomesteadSigner{}
		} else {
			signer = types.LatestSignerForChainID(tx.ChainId())
		}
		fromAddr, err := types.Sender(signer, tx)
		if err != nil {
			s.logger.Error("Failed to decode tx", "tx", tx.Hash().Hex(), "err", err)
			continue
		}

		from := strings.ToLower(fromAddr.Hex())
		var to string
		if tx.To() != nil {
			to = strings.ToLower(tx.To().Hex())
		}

		amountWei := new(big.Int).Set(tx.Value())
		amountEth := weiToEthString(amountWei)

		// Match against known addresses
		if userID, ok := s.addresses[from]; ok {
			s.handleMatch(ctx, userID, from, to, amountEth, tx.Hash().Hex(), block.NumberU64(), "FROM")
		}
		if to != "" {
			if userID, ok := s.addresses[to]; ok {
				s.handleMatch(ctx, userID, from, to, amountEth, tx.Hash().Hex(), block.NumberU64(), "TO")
			}
		}
	}

	s.logger.Debug("Finished processing block", "block", block.NumberU64())
}

func (s *Scanner) handleMatch(ctx context.Context, userID, from, to, amount, txHash string, blockNum uint64, direction string) {
	s.logger.Info("Matched address",
		"direction", direction,
		"user", userID,
		"from", from,
		"to", to,
		"block", blockNum,
		"tx", txHash,
	)

	s.publishMatch(ctx, userID, from, to, amount, txHash, blockNum)
}

func (s *Scanner) publishMatch(ctx context.Context, userID, from, to, amountEth string, hash string, blockNum uint64) {
	if s.publisher == nil || s.matchTopic == "" {
		return
	}

	ev := models.TxMatchedEvent{
		UserID:      userID,
		From:        from,
		To:          to,
		Amount:      amountEth,
		Hash:        hash,
		BlockNumber: blockNum,
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		s.logger.Error("Failed to marshal match event", "user", userID, "tx", hash, "err", err)
		return
	}

	if err := s.publisher.Publish(ctx, s.matchTopic, []byte(userID), payload); err != nil {
		s.logger.Error("Failed to publish match event", "user", userID, "tx", hash, "err", err)
		s.recordFailedEvent(ev)
	}
}

func (s *Scanner) recordFailedBlock(blockNum uint64) {
	s.failedBlocksMu.Lock()
	defer s.failedBlocksMu.Unlock()
	s.failedBlockNums = append(s.failedBlockNums, blockNum)

	s.logger.Warn("Recorded failed block",
		"block", blockNum,
		"totalFailed", len(s.failedBlockNums),
	)
}

func (s *Scanner) recordFailedEvent(ev models.TxMatchedEvent) {
	s.failedEventsMu.Lock()
	defer s.failedEventsMu.Unlock()
	s.failedEvents = append(s.failedEvents, ev)

	s.logger.Warn("Recorded failed publish event",
		"user", ev.UserID,
		"tx", ev.Hash,
		"block", ev.BlockNumber,
		"totalFailed", len(s.failedEvents),
	)
}

func weiToEthString(wei *big.Int) string {
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1e18
	q, r := new(big.Int).QuoRem(wei, den, new(big.Int))
	// Format as "<q>.<r left-padded to 18>"
	rs := r.String()
	// left-pad r to 18 digits
	if len(rs) < 18 {
		rs = strings.Repeat("0", 18-len(rs)) + rs
	}

	// Trim trailing 0's
	rs = strings.TrimRight(rs, "0")
	if rs == "" {
		return q.String()
	}
	return q.String() + "." + rs
}
