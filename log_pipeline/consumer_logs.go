package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	flushInterval     = 1 * time.Second
	shutdownTimeout   = 10 * time.Second
	initialRetryDelay = 500 * time.Millisecond
	maxRetryDelay     = 30 * time.Second
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
		CommitInterval: 0,    // commit synchronously, and only after a successful flush
		StartOffset:    kafka.FirstOffset,
	})

	if bufferSize < 1 {
		bufferSize = 1
	}
	return &LogConsumer{
		reader:     reader,
		storage:    storage,
		bufferSize: bufferSize,
	}
}

// batch holds fetched-but-not-yet-persisted messages. Offsets are committed
// only after the documents have been written to Elasticsearch, so a crash or
// an ES outage results in redelivery rather than data loss (at-least-once).
// Writes are idempotent because every document gets a deterministic _id.
type batch struct {
	msgs []kafka.Message
	docs []map[string]interface{}
}

func (b *batch) reset() {
	b.msgs = b.msgs[:0]
	b.docs = b.docs[:0]
}

func (b *batch) empty() bool { return len(b.msgs) == 0 }

func (c *LogConsumer) Consume(ctx context.Context) {
	log.Printf("[kafka_consumer] logs consumer started")
	b := &batch{}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[kafka_consumer] stopping logs consumer")
			c.drain(b)
			if err := c.reader.Close(); err != nil {
				log.Printf("[kafka_consumer] error closing logs reader: %v", err)
			}
			return

		case <-ticker.C:
			if !b.empty() {
				c.flush(ctx, b)
			}

		default:
			// Fetch with a short timeout so the select loop stays responsive.
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				if err != context.DeadlineExceeded && ctx.Err() == nil {
					log.Printf("[kafka_consumer] read error: %v", err)
				}
				continue
			}

			// Always track the message so its offset is committed together with
			// the batch (committing a later offset implicitly commits earlier ones).
			b.msgs = append(b.msgs, msg)

			var logMsg map[string]interface{}
			if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
				log.Printf("[kafka_consumer] bad log message json: %v, body: %s", err, string(msg.Value))
			} else {
				b.docs = append(b.docs, logMsg)
			}

			if len(b.msgs) >= c.bufferSize {
				c.flush(ctx, b)
			}
		}
	}
}

// flush persists the batch and commits its offsets. On failure it retries with
// exponential backoff until it succeeds or ctx is cancelled; in the latter
// case nothing is committed and the messages are redelivered on restart.
func (c *LogConsumer) flush(ctx context.Context, b *batch) {
	delay := initialRetryDelay
	for {
		err := c.storage.StoreLogs(ctx, b.docs)
		if err == nil {
			break
		}
		log.Printf("[kafka_consumer] failed to store %d logs (retrying in %s): %v", len(b.docs), delay, err)
		select {
		case <-ctx.Done():
			log.Printf("[kafka_consumer] shutdown during retry; %d messages left uncommitted for redelivery", len(b.msgs))
			return
		case <-time.After(delay):
		}
		if delay *= 2; delay > maxRetryDelay {
			delay = maxRetryDelay
		}
	}

	if err := c.reader.CommitMessages(ctx, b.msgs...); err != nil {
		// Documents are already stored; on redelivery they are overwritten
		// idempotently by _id, so this is safe.
		log.Printf("[kafka_consumer] error committing offsets: %v", err)
	}
	b.reset()
}

// drain does a final, bounded flush on shutdown using a fresh context (the
// consumer context is already cancelled at this point).
func (c *LogConsumer) drain(b *batch) {
	if b.empty() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := c.storage.StoreLogs(ctx, b.docs); err != nil {
		log.Printf("[kafka_consumer] final flush failed, %d messages left uncommitted for redelivery: %v", len(b.msgs), err)
		return
	}
	if err := c.reader.CommitMessages(ctx, b.msgs...); err != nil {
		log.Printf("[kafka_consumer] final commit failed: %v", err)
	}
	b.reset()
}
