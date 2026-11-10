// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnectClientOffsets(t *testing.T) {
	t.Run("read requested topic partition", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/connectors/fixture/offsets" {
				t.Errorf("request = %s %s, want GET /connectors/fixture/offsets", r.Method, r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"offsets":[{"partition":{"kafka_topic":"other","kafka_partition":0},"offset":{"kafka_offset":2}},{"partition":{"kafka_topic":"mo_issue_29848_events","kafka_partition":0},"offset":{"kafka_offset":17}}]}`))
		}))
		defer server.Close()

		got, err := (connectClient{base: server.URL, client: server.Client()}).restOffset(t.Context(), "fixture", topicName, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != 17 {
			t.Fatalf("offset = %d, want 17", got)
		}
	})

	t.Run("reject missing partition offset", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"offsets":[{"partition":{"kafka_topic":"mo_issue_29848_events","kafka_partition":0},"offset":null}]}`))
		}))
		defer server.Close()

		_, err := (connectClient{base: server.URL, client: server.Client()}).restOffset(t.Context(), "fixture", topicName, 0)
		if err == nil || !strings.Contains(err.Error(), "omit "+topicName+"[0]") {
			t.Fatalf("restOffset error = %v, want missing partition offset", err)
		}
	})

	t.Run("patch only the requested partition and offset", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPatch || r.URL.Path != "/connectors/fixture/offsets" {
				t.Errorf("request = %s %s, want PATCH /connectors/fixture/offsets", r.Method, r.URL.Path)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			var body connectorOffsets
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode PATCH body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(body.Offsets) != 1 || body.Offsets[0].Partition.KafkaTopic != topicName || body.Offsets[0].Partition.KafkaPartition != 0 || body.Offsets[0].Offset == nil || body.Offsets[0].Offset.KafkaOffset != 9 {
				t.Errorf("PATCH body = %+v, want only %s[0] offset 9", body, topicName)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		if err := (connectClient{base: server.URL, client: server.Client()}).patchOffset(t.Context(), "fixture", topicName, 0, 9); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reject unexpected patch response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "connector must be stopped", http.StatusConflict)
		}))
		defer server.Close()

		err := (connectClient{base: server.URL, client: server.Client()}).patchOffset(t.Context(), "fixture", topicName, 0, 9)
		if err == nil || !strings.Contains(err.Error(), "HTTP 409") {
			t.Fatalf("patchOffset error = %v, want HTTP 409", err)
		}
	})
}
