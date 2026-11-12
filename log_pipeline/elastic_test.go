package main

import (
	"strings"
	"testing"
)

func TestCheckBulkResponse(t *testing.T) {
	s := &ElasticsearchStorage{}
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"all ok", `{"errors":false,"items":[{"index":{"_id":"a","status":201}}]}`, false},
		{"retryable 429", `{"errors":true,"items":[{"index":{"_id":"a","status":429,"error":{"type":"es_rejected_execution_exception"}}}]}`, true},
		{"retryable 503", `{"errors":true,"items":[{"index":{"_id":"a","status":503}}]}`, true},
		{"permanent 400 is skipped", `{"errors":true,"items":[{"index":{"_id":"a","status":400,"error":{"type":"mapper_parsing_exception"}}}]}`, false},
		{"mixed permanent+retryable retries", `{"errors":true,"items":[{"index":{"_id":"a","status":400}},{"index":{"_id":"b","status":500}}]}`, true},
		{"garbage body", `not json`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.checkBulkResponse(strings.NewReader(tc.body), 2)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestGeneratedLogIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 10000; i++ {
		id := nextLogID()
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
}
