package tkafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	mu       sync.RWMutex
	writers  map[string]*kafka.Writer
	brokers  []string
	balancer kafka.Balancer
	acks     kafka.RequiredAcks

	allowAutoTopicCreation bool
	async                  bool

	// Tunables (optional; 0 means use kafka-go defaults)
	batchBytes   int64
	batchSize    int
	batchTimeout time.Duration
}

type Config struct {
	Brokers                []string
	RequiredAcks           kafka.RequiredAcks // e.g. kafka.RequireAll
	AllowAutoTopicCreation bool
	Async                  bool

	// Optional
	BatchBytes   int64
	BatchSize    int
	BatchTimeout time.Duration
}

func NewKafkaPublisher(cfg Config) *KafkaPublisher {
	return &KafkaPublisher{
		writers:                make(map[string]*kafka.Writer),
		brokers:                cfg.Brokers,
		balancer:               &kafka.Hash{},
		acks:                   cfg.RequiredAcks,
		allowAutoTopicCreation: cfg.AllowAutoTopicCreation,
		async:                  cfg.Async,

		batchBytes:   cfg.BatchBytes,
		batchSize:    cfg.BatchSize,
		batchTimeout: cfg.BatchTimeout,
	}
}

func (p *KafkaPublisher) getWriter(topic string) *kafka.Writer {
	p.mu.RLock()
	w := p.writers[topic]
	p.mu.RUnlock()
	if w != nil {
		return w
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if w = p.writers[topic]; w != nil {
		return w
	}

	w = &kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  topic,
		Balancer:               p.balancer,
		RequiredAcks:           p.acks,
		Async:                  p.async,
		AllowAutoTopicCreation: p.allowAutoTopicCreation,
	}

	// Apply optionals
	if p.batchTimeout > 0 {
		w.BatchTimeout = p.batchTimeout
	}
	if p.batchBytes > 0 {
		w.BatchBytes = p.batchBytes
	}
	if p.batchSize > 0 {
		w.BatchSize = p.batchSize
	}

	p.writers[topic] = w
	return w
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	w := p.getWriter(topic)
	return w.WriteMessages(ctx, kafka.Message{
		Key:   key, // user_id as key
		Value: value,
	})
}

func (p *KafkaPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for topic, w := range p.writers {
		if err := w.Close(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close writer for topic %q: %w", topic, err)
			}
		}
	}
	return firstErr
}
