// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "testing"

func TestCreateNamespaceURLUsesNegotiatedPrefix(t *testing.T) {
	tests := []struct {
		name    string
		base    string
		prefix  string
		want    string
		wantErr bool
	}{
		{
			name:   "adds v1 and prefix",
			base:   "http://127.0.0.1:19120/iceberg",
			prefix: "main",
			want:   "http://127.0.0.1:19120/iceberg/v1/main/namespaces",
		},
		{
			name:   "does not duplicate existing v1",
			base:   "http://127.0.0.1:19120/iceberg/v1",
			prefix: "main",
			want:   "http://127.0.0.1:19120/iceberg/v1/main/namespaces",
		},
		{
			name:   "escapes composite prefix as one segment",
			base:   "http://127.0.0.1:19120/iceberg",
			prefix: "main|s3://warehouse",
			want:   "http://127.0.0.1:19120/iceberg/v1/main%7Cs3:%2F%2Fwarehouse/namespaces",
		},
		{
			name: "supports catalogs without prefix",
			base: "http://127.0.0.1:19120/iceberg",
			want: "http://127.0.0.1:19120/iceberg/v1/namespaces",
		},
		{
			name:    "rejects invalid uri",
			base:    "://bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createNamespaceURL(tt.base, tt.prefix)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected URL: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestSnapshotRetainedInMetadata(t *testing.T) {
	tests := []struct {
		name       string
		metadata   string
		snapshotID int64
		want       bool
		wantErr    bool
	}{
		{
			name: "retained",
			metadata: `{
				"snapshots": [
					{"snapshot-id": 101},
					{"snapshot-id": 102}
				]
			}`,
			snapshotID: 101,
			want:       true,
		},
		{
			name: "not retained",
			metadata: `{
				"snapshots": [
					{"snapshot-id": 102}
				]
			}`,
			snapshotID: 101,
			want:       false,
		},
		{
			name:       "empty metadata",
			snapshotID: 101,
			wantErr:    true,
		},
		{
			name:       "invalid metadata",
			metadata:   `{`,
			snapshotID: 101,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := snapshotRetainedInMetadata([]byte(tt.metadata), tt.snapshotID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected retained result: got %v want %v", got, tt.want)
			}
		})
	}
}
