package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"

	"github.com/cerisilv123/ethereum-monitor/internal/config"
	"github.com/cerisilv123/ethereum-monitor/internal/service"
	"github.com/cerisilv123/ethereum-monitor/internal/transport/ethereum"
	"github.com/cerisilv123/ethereum-monitor/internal/transport/tkafka"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	if err := godotenv.Load(); err != nil {
		logger.Warn("No .env file found, falling back to system environment")
	}

	// Load config (from env, with defaults)
	cfg := config.MustFromEnv()

	// Load user addresses
	addresses, err := service.LoadUserAddressMapFile(cfg.UserAddressFile)
	if err != nil {
		logger.Error("Failed to load addresses",
			"file", cfg.UserAddressFile,
			"error", err,
		)
		os.Exit(1)
	}

	logger.Info("Loaded user addresses",
		"count", len(addresses),
	)

	// Connect to RPC client
	rpcClient, err := ethereum.NewMultiRPCClient(cfg.RPCURLs, logger, cfg.NodeRetryAttempts, cfg.NodeRetryBaseDelay)
	if err != nil {
		logger.Error("Failed to connect to Ethereum RPC", "error", err)
		os.Exit(1)
	}
	defer rpcClient.Close()

	// Connect to kafka producer
	kpub := tkafka.NewKafkaPublisher(tkafka.Config{
		Brokers:                cfg.KafkaBrokers,
		RequiredAcks:           kafka.RequireAll, // tune as needed
		AllowAutoTopicCreation: cfg.KafkaAutoCreate,
		Async:                  cfg.KafkaAsync,
		BatchBytes:             int64(cfg.KafkaBatchBytes),
		BatchSize:              cfg.KafkaBatchSize,
		BatchTimeout:           cfg.KafkaBatchTimeout,
	})
	defer kpub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scanner := service.NewScanner(service.ScannerConfig{
		Logger:        logger,
		Client:        rpcClient,
		Addresses:     addresses,
		WorkerThreads: cfg.WorkerThreads,
		BufferSize:    cfg.TransactionBuffer,
		Publisher:     kpub,
		MatchTopic:    cfg.KafkaTopic,
		RPCTimeout:    cfg.RPCTimeout,
		RetryInterval: cfg.RetryInterval,
	})

	// Connect to service
	svc := service.NewService(scanner, logger)

	go func() {
		if err := svc.Scanner.Run(ctx); err != nil {
			svc.Logger.Error("Error running scanner", "error", err)
			os.Exit(1)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	logger.Info("Shutdown signal received")

	cancel()

	logger.Info("Scanner shut down cleanly")
}
