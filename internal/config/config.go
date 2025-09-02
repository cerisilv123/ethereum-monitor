package config

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddressesFilePath = "user_addresses.csv"
	defaultWorkers           = 3
	defaultTxBuffer          = 1000

	// Kafka defaults
	defaultKafkaTopic        = "ethereum_transactions"
	defaultKafkaAcks         = "all"
	defaultKafkaAutoCreate   = true
	defaultKafkaAsync        = false
	defaultKafkaBatchTimeout = 10 * time.Millisecond

	// Ethereum Client
	defaultRPCTimeout         = 10 * time.Second
	defaultRetryInterval      = 30 * time.Second
	defaultNodeRetryAttempts  = 2
	defaultNodeRetryBaseDelay = 100 * time.Millisecond
)

type Config struct {
	UserAddressFile   string
	WorkerThreads     int
	TransactionBuffer int

	KafkaBrokers      []string
	KafkaTopic        string
	KafkaAcks         string
	KafkaAutoCreate   bool
	KafkaAsync        bool
	KafkaBatchBytes   int
	KafkaBatchSize    int
	KafkaBatchTimeout time.Duration

	RPCURLs            []string
	RPCTimeout         time.Duration
	RetryInterval      time.Duration
	NodeRetryAttempts  int
	NodeRetryBaseDelay time.Duration
}

func FromEnv() (*Config, error) {
	cfg := &Config{
		UserAddressFile:   getString("USER_ADDRESS_FILE", defaultAddressesFilePath),
		RPCURLs:           getList("RPC_URLS", ","),
		WorkerThreads:     getInt("WORKER_THREADS", defaultWorkers),
		TransactionBuffer: getInt("TRANSACTION_BUFFER", defaultTxBuffer),

		KafkaBrokers:      getList("KAFKA_BROKERS", ","),
		KafkaTopic:        getString("KAFKA_TOPIC", defaultKafkaTopic),
		KafkaAcks:         strings.ToLower(getString("KAFKA_ACKS", defaultKafkaAcks)),
		KafkaAutoCreate:   getBool("KAFKA_AUTO_CREATE", defaultKafkaAutoCreate),
		KafkaAsync:        getBool("KAFKA_ASYNC", defaultKafkaAsync),
		KafkaBatchBytes:   getInt("KAFKA_BATCH_BYTES", 0),
		KafkaBatchSize:    getInt("KAFKA_BATCH_SIZE", 0),
		KafkaBatchTimeout: getDuration("KAFKA_BATCH_TIMEOUT", defaultKafkaBatchTimeout),

		RPCTimeout:         getDuration("RPC_TIMEOUT", defaultRPCTimeout),
		RetryInterval:      getDuration("RETRY_INTERVAL", defaultRetryInterval),
		NodeRetryAttempts:  getInt("NODE_RETRY_ATTEMPTS", defaultNodeRetryAttempts),
		NodeRetryBaseDelay: getDuration("NODE_RETRY_BASE_DELAY", defaultNodeRetryBaseDelay),
	}
	return cfg, cfg.Validate()
}

func MustFromEnv() *Config {
	cfg, err := FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func (c *Config) Validate() error {
	var errs []string
	if strings.TrimSpace(c.UserAddressFile) == "" {
		errs = append(errs, "USER_ADDRESS_FILE is required")
	}
	if len(c.RPCURLs) == 0 {
		errs = append(errs, "RPC_URLS is required (comma-separated)")
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, "KAFKA_BROKERS is required (comma-separated)")
	}
	if strings.TrimSpace(c.KafkaTopic) == "" {
		errs = append(errs, "KAFKA_TOPIC is required")
	}
	if c.WorkerThreads <= 0 {
		errs = append(errs, "WORKER_THREADS must be > 0")
	}
	if c.TransactionBuffer <= 0 {
		errs = append(errs, "TRANSACTION_BUFFER must be > 0")
	}
	if len(errs) > 0 {
		return errors.New("config error: " + strings.Join(errs, "; "))
	}
	return nil
}

func getString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func getInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	i, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		log.Printf("WARN: %s=%q is not an int; using default %d", key, v, def)
		return def
	}
	return i
}

func getBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "t", "true", "yes", "y":
		return true
	case "0", "f", "false", "no", "n":
		return false
	default:
		log.Printf("WARN: %s=%q is not a bool; using default %v", key, v, def)
		return def
	}
}

func getList(key, sep string) []string {
	raw := getString(key, "")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func getDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		log.Printf("WARN: %s=%q is not a duration; using default %s", key, v, def.String())
		return def
	}
	return d
}
