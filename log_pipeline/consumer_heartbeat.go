package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

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
	log.Printf("[heartbeat_kafka] consumer started")
	for {
		select {
		case <-ctx.Done():
			log.Printf("[heartbeat_kafka] stopping heartbeat consumer")
			if err := c.reader.Close(); err != nil {
				log.Printf("[heartbeat_kafka] error closing reader: %v", err)
			}
			return

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
			serviceName, _ := hbMsg["service_name"].(string)
			status, _ := hbMsg["status"].(string)

			if nodeID == "" {
				nodeID = "unknown"
			}
			if serviceName == "" {
				serviceName = "unknown"
			}

			mappedStatus := "inactive"
			if status == "UP" {
				mappedStatus = "active"
			}

			if err := c.storage.UpsertNodeStatus(ctx, nodeID, serviceName, mappedStatus); err != nil {
				log.Printf("[heartbeat_kafka] failed to upsert status: %v", err)
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[heartbeat_kafka] error committing offset: %v", err)
			}
		}
	}
}
