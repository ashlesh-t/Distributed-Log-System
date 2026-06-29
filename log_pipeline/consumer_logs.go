package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type LogConsumer struct {
	reader     *kafka.Reader
	storage    *ElasticsearchStorage
	bufferSize int
}

func NewLogConsumer(brokers []string, topic string, groupID string, storage *ElasticsearchStorage, bufferSize int) *LogConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &LogConsumer{
		reader:     reader,
		storage:    storage,
		bufferSize: bufferSize,
	}
}

func (c *LogConsumer) Consume(ctx context.Context) {
	log.Printf("[kafka_consumer] logs consumer started")
	buffer := make([]map[string]interface{}, 0, c.bufferSize)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[kafka_consumer] stopping logs consumer")
			// Flush remaining buffer before exiting
			if len(buffer) > 0 {
				c.flush(ctx, buffer)
			}
			if err := c.reader.Close(); err != nil {
				log.Printf("[kafka_consumer] error closing logs reader: %v", err)
			}
			return

		case <-ticker.C:
			if len(buffer) > 0 {
				c.flush(ctx, buffer)
				buffer = make([]map[string]interface{}, 0, c.bufferSize)
			}

		default:
			// Fetch next message with timeout to avoid blocking main select loop indefinitely
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				// Don't log deadline exceeded errors as they occur during idle periods
				if err != context.DeadlineExceeded && ctx.Err() == nil {
					log.Printf("[kafka_consumer] read error: %v", err)
				}
				continue
			}

			var logMsg map[string]interface{}
			if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
				log.Printf("[kafka_consumer] bad log message json: %v, body: %s", err, string(msg.Value))
				c.reader.CommitMessages(ctx, msg)
				continue
			}

			buffer = append(buffer, logMsg)

			if len(buffer) >= c.bufferSize {
				c.flush(ctx, buffer)
				buffer = make([]map[string]interface{}, 0, c.bufferSize)
			}

			// Commit read message
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[kafka_consumer] error committing offset: %v", err)
			}
		}
	}
}

func (c *LogConsumer) flush(ctx context.Context, buffer []map[string]interface{}) {
	if err := c.storage.StoreLogs(ctx, buffer); err != nil {
		log.Printf("[kafka_consumer] failed to store logs: %v", err)
	}
}
