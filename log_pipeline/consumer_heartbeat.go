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
	reader  *kafka.Reader
	storage *ElasticsearchStorage
}

func NewHeartbeatConsumer(brokers []string, topic string, groupID string, storage *ElasticsearchStorage) *HeartbeatConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &HeartbeatConsumer{
		reader:  reader,
		storage: storage,
	}
}

func (c *HeartbeatConsumer) Consume(ctx context.Context) {
	log.Printf("[heartbeat_kafka] consumer started — indexing heartbeats into distributed_logs-*")
	buffer := make([]map[string]interface{}, 0, 50)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[heartbeat_kafka] stopping heartbeat consumer")
			// Flush any remaining buffered heartbeats before exiting
			if len(buffer) > 0 {
				c.storage.StoreLogs(ctx, buffer)
			}
			if err := c.reader.Close(); err != nil {
				log.Printf("[heartbeat_kafka] error closing reader: %v", err)
			}
			return

		case <-ticker.C:
			// Periodic flush every 2 seconds
			if len(buffer) > 0 {
				c.storage.StoreLogs(ctx, buffer)
				buffer = make([]map[string]interface{}, 0, 50)
			}

		default:
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				if err != context.DeadlineExceeded && ctx.Err() == nil {
					log.Printf("[heartbeat_kafka] read error: %v", err)
				}
				continue
			}

			var hbMsg map[string]interface{}
			if err := json.Unmarshal(msg.Value, &hbMsg); err != nil {
				log.Printf("[heartbeat_kafka] bad heartbeat JSON: %v, body: %s", err, string(msg.Value))
				c.reader.CommitMessages(ctx, msg)
				continue
			}

			nodeID, _ := hbMsg["node_id"].(string)
			if nodeID == "" {
				nodeID = "unknown"
			}
			log.Printf("[heartbeat_kafka] received heartbeat from node %s — buffering for ES", nodeID)

			// Buffer the heartbeat for bulk indexing into distributed_logs-*
			buffer = append(buffer, hbMsg)

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[heartbeat_kafka] error committing offset: %v", err)
			}
		}
	}
}
