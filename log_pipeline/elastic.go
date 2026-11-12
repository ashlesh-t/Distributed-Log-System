package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// logIDSeq disambiguates log IDs generated within the same nanosecond.
var logIDSeq uint64

func nextLogID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddUint64(&logIDSeq, 1))
}

type ElasticsearchStorage struct {
	client      *elasticsearch.Client
	indexPrefix string
	shards      int // primary shards per newly created node index
	replicas    int
}

// NewElasticsearchStorage builds the storage layer. shards/replicas apply to
// indices created after the template is installed (existing indices keep
// their shard count; ES cannot change it in place).
func NewElasticsearchStorage(host string, shards, replicas int) (*ElasticsearchStorage, error) {
	if shards < 1 {
		shards = 1
	}
	if replicas < 0 {
		replicas = 0
	}
	cfg := elasticsearch.Config{
		Addresses: []string{host},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchStorage{
		client:      client,
		indexPrefix: "distributed_logs",
		shards:      shards,
		replicas:    replicas,
	}, nil
}

func (s *ElasticsearchStorage) EnsureTemplate(ctx context.Context) error {
	templateBody := map[string]interface{}{
		"index_patterns": []string{s.indexPrefix + "-*"},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   s.shards,
				"number_of_replicas": s.replicas,
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"node_id":            map[string]string{"type": "keyword"},
					"message_type":       map[string]string{"type": "keyword"},
					"service_name":       map[string]string{"type": "keyword"},
					"api_key_hash":       map[string]string{"type": "keyword"},
					"timestamp":          map[string]string{"type": "date"},
					"log_id":             map[string]string{"type": "keyword"},
					"log_level":          map[string]string{"type": "keyword"},
					"message":            map[string]string{"type": "text"},
					"response_time_ms":   map[string]string{"type": "integer"},
					"threshold_limit_ms": map[string]string{"type": "integer"},
					"error_details": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"error_code":    map[string]string{"type": "keyword"},
							"error_message": map[string]string{"type": "text"},
						},
					},
				},
			},
		},
	}

	bodyBytes, err := json.Marshal(templateBody)
	if err != nil {
		return err
	}

	req := esapi.IndicesPutIndexTemplateRequest{
		Name: s.indexPrefix + "_template",
		Body: bytes.NewReader(bodyBytes),
	}

	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error putting template: %s", res.String())
	}

	log.Printf("[elastic] index template ensured: %s_template", s.indexPrefix)
	return nil
}

func (s *ElasticsearchStorage) StoreLogs(ctx context.Context, logs []map[string]interface{}) error {
	if len(logs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, logMsg := range logs {
		// Generate log_id if not present
		logID, ok := logMsg["log_id"].(string)
		if !ok || logID == "" {
			logID = nextLogID()
			logMsg["log_id"] = logID
		}

		nodeID, _ := logMsg["node_id"].(string)
		if nodeID == "" {
			nodeID = "unknown"
		}
		indexName := fmt.Sprintf("%s-%s", s.indexPrefix, nodeID)

		// Create metadata line
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    logID,
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		// Create source doc line
		docBytes, err := json.Marshal(logMsg)
		if err != nil {
			return err
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk store logs failed: %s", res.String())
	}

	return s.checkBulkResponse(res.Body, len(logs))
}

// checkBulkResponse inspects per-item results. Retryable failures (429/5xx)
// are returned as an error so the caller retries the whole batch (safe: _id is
// deterministic). Permanent failures (e.g. 400 mapping errors) are logged with
// detail and skipped so a single poison document cannot block the pipeline.
func (s *ElasticsearchStorage) checkBulkResponse(body io.Reader, sent int) error {
	var resp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			ID     string          `json:"_id"`
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return fmt.Errorf("decoding bulk response: %w", err)
	}
	if !resp.Errors {
		log.Printf("[elastic] bulk index stored %d logs successfully", len(resp.Items))
		return nil
	}

	retryable, dropped := 0, 0
	for _, item := range resp.Items {
		for _, r := range item {
			if r.Status < 300 {
				continue
			}
			if r.Status == 429 || r.Status >= 500 {
				retryable++
				continue
			}
			dropped++
			log.Printf("[elastic] dropping log %s: permanent bulk error (status %d): %s", r.ID, r.Status, string(r.Error))
		}
	}
	if retryable > 0 {
		return fmt.Errorf("%d of %d bulk items failed with a retryable error", retryable, sent)
	}
	log.Printf("[elastic] bulk index stored %d logs, dropped %d unindexable", sent-dropped, dropped)
	return nil
}
