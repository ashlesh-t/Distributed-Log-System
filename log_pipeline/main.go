package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

func main() {
	log.Println("[log_pipeline] starting Go pipeline daemon...")

	// Read environment variables
	kafkaServersEnv := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaServersEnv == "" {
		kafkaServersEnv = "kafka:9092"
	}
	brokers := strings.Split(kafkaServersEnv, ",")

	esHost := os.Getenv("ELASTICSEARCH_HOST")
	if esHost == "" {
		esHost = "http://elasticsearch:9200"
	}

	bufferSize := 10
	if bufSizeStr := os.Getenv("LOG_BUFFER_SIZE"); bufSizeStr != "" {
		if val, err := strconv.Atoi(bufSizeStr); err == nil {
			bufferSize = val
		}
	}

	// Initialize Elasticsearch storage
	storage, err := NewElasticsearchStorage(esHost)
	if err != nil {
		log.Fatalf("[log_pipeline] failed to initialize ES: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ensure the index template mappings are created in Elasticsearch
	if err := storage.EnsureTemplate(ctx); err != nil {
		log.Printf("[log_pipeline] error ensuring ES templates: %v", err)
	}

	// Start consumers
	logConsumer := NewLogConsumer(brokers, "logs", "log-consumer-group", storage, bufferSize)
	hbConsumer := NewHeartbeatConsumer(brokers, "heartbeat", "heartbeat-storage-group")

	// Run goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); logConsumer.Consume(ctx) }()
	go func() { defer wg.Done(); hbConsumer.Consume(ctx) }()

	// Handle os signal termination gracefully
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs
	log.Printf("[log_pipeline] received termination signal: %v. Initiating graceful shutdown...", sig)
	cancel()

	// Wait for consumers to perform their final flush and commit
	wg.Wait()
	log.Println("[log_pipeline] shutdown complete")
}
