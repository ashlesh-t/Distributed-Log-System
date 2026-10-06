package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// HeartbeatConsumer consumes the heartbeat Kafka topic and indexes each event
// into distributed_logs-* for heartbeat history queries via GET /v1/nodes/{id}/heartbeats.
// It does NOT write node_status — heartbeat_service owns failure detection so
// that active/failed status doesn't flap between two concurrent writers.
type HeartbeatConsumer struct {
	reader     *kafka.Reader
	storage    *ElasticsearchStorage
	bufferSize int
}

func NewHeartbeatConsumer(brokers []string, topic string, groupID string, storage *ElasticsearchStorage, bufferSize int) *HeartbeatConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &HeartbeatConsumer{
		reader:     reader,
		storage:    storage,
		bufferSize: bufferSize,
	}
}

func (c *HeartbeatConsumer) Consume(ctx context.Context) {
	// Emitting as JSON just in case this service's logs are ever routed to Fluentd
	log.Printf(`{"message_type":"SYSTEM","log_level":"INFO","message":"[heartbeat_kafka] consumer started — indexing heartbeats"}`)
	buffer := make([]map[string]interface{}, 0, c.bufferSize)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf(`{"message_type":"SYSTEM","log_level":"INFO","message":"[heartbeat_kafka] stopping heartbeat consumer"}`)
			// Flush any remaining buffered heartbeats before exiting
			if len(buffer) > 0 {
				c.storage.StoreLogs(ctx, buffer)
				buffer = nil // explicitly free
			}
			if err := c.reader.Close(); err != nil {
				log.Printf(`{"message_type":"SYSTEM","log_level":"ERROR","message":"[heartbeat_kafka] error closing reader: %v"}`, err)
			}
			return

		case <-ticker.C:
			// Periodic flush every 2 seconds
			if len(buffer) > 0 {
				c.storage.StoreLogs(ctx, buffer)
				// clear buffer but retain capacity to avoid reallocation (GC friendly)
				buffer = buffer[:0]
			}

		default:
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				if err != context.DeadlineExceeded && ctx.Err() == nil {
					log.Printf(`{"message_type":"SYSTEM","log_level":"ERROR","message":"[heartbeat_kafka] read error: %v"}`, err)
				}
				continue
			}

			var hbMsg map[string]interface{}
			if err := json.Unmarshal(msg.Value, &hbMsg); err != nil {
				log.Printf(`{"message_type":"SYSTEM","log_level":"ERROR","message":"[heartbeat_kafka] bad heartbeat JSON: %v, body: %s"}`, err, string(msg.Value))
				c.reader.CommitMessages(ctx, msg)
				continue
			}

			buffer = append(buffer, hbMsg)

			// PREVENT OOM: Flush immediately if buffer reaches capacity limit
			if len(buffer) >= c.bufferSize {
				c.storage.StoreLogs(ctx, buffer)
				buffer = buffer[:0] // clear buffer after flush
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf(`{"message_type":"SYSTEM","log_level":"ERROR","message":"[heartbeat_kafka] error committing offset: %v"}`, err)
			}
		}
	}
}
