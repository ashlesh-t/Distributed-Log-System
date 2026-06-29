package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticsearchStorage struct {
	client      *elasticsearch.Client
	indexPrefix string
}

func NewElasticsearchStorage(host string) (*ElasticsearchStorage, error) {
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
	}, nil
}

func (s *ElasticsearchStorage) EnsureTemplate(ctx context.Context) error {
	templateBody := map[string]interface{}{
		"index_patterns": []string{s.indexPrefix + "-*"},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   1,
				"number_of_replicas": 1,
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
			logID = fmt.Sprintf("%d-%f", time.Now().UnixNano(), time.Now().Sub(time.Time{}).Seconds())
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

	// Read response to log status
	var rawResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&rawResponse); err == nil {
		if errors, ok := rawResponse["errors"].(bool); ok && errors {
			log.Printf("[elastic] bulk index warned of partial errors")
		} else {
			items, _ := rawResponse["items"].([]interface{})
			log.Printf("[elastic] bulk index stored %d logs successfully", len(items))
		}
	}

	return nil
}

func (s *ElasticsearchStorage) UpsertNodeStatus(ctx context.Context, nodeID string, serviceName string, status string) error {
	doc := map[string]interface{}{
		"node_id":      nodeID,
		"service_name": serviceName,
		"status":       status,
		"updated_at":   time.Now().UTC().Format(time.RFC3339),
	}

	payload := map[string]interface{}{
		"doc":           doc,
		"doc_as_upsert": true,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req := esapi.UpdateRequest{
		Index:      "node_status",
		DocumentID: nodeID,
		Body:       bytes.NewReader(bodyBytes),
	}

	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("upsert node status failed: %s", res.String())
	}

	return nil
}
