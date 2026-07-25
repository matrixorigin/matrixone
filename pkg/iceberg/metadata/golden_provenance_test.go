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

package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type goldenVectorProvenance struct {
	SchemaVersion string                 `json:"schema_version"`
	Vectors       []goldenVectorArtifact `json:"vectors"`
}

type goldenVectorArtifact struct {
	ID                string   `json:"id"`
	Artifact          string   `json:"artifact"`
	SHA256            string   `json:"sha256"`
	Generator         string   `json:"generator"`
	GenerationCommand string   `json:"generation_command"`
	SourceVersions    []string `json:"source_versions"`
	InputSchemas      []string `json:"input_schemas"`
}

func TestGoldenVectorProvenanceArtifacts(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "golden_vectors_provenance.json"))
	if err != nil {
		t.Fatalf("read golden vector provenance: %v", err)
	}
	var provenance goldenVectorProvenance
	if err := json.Unmarshal(data, &provenance); err != nil {
		t.Fatalf("decode golden vector provenance: %v", err)
	}
	if provenance.SchemaVersion == "" || len(provenance.Vectors) == 0 {
		t.Fatalf("provenance must declare schema version and vectors: %+v", provenance)
	}
	seen := make(map[string]bool, len(provenance.Vectors))
	for _, vector := range provenance.Vectors {
		if vector.ID == "" || vector.Artifact == "" || vector.SHA256 == "" ||
			vector.Generator == "" || vector.GenerationCommand == "" ||
			len(vector.SourceVersions) == 0 || len(vector.InputSchemas) == 0 {
			t.Fatalf("golden vector provenance is incomplete: %+v", vector)
		}
		if seen[vector.ID] {
			t.Fatalf("duplicate golden vector provenance id: %s", vector.ID)
		}
		seen[vector.ID] = true
		artifact, err := os.ReadFile(filepath.Join("testdata", vector.Artifact))
		if err != nil {
			t.Fatalf("read golden vector artifact %s: %v", vector.Artifact, err)
		}
		digest := sha256.Sum256(artifact)
		if got := hex.EncodeToString(digest[:]); got != strings.ToLower(vector.SHA256) {
			t.Fatalf("sha256 mismatch for %s: got %s want %s", vector.Artifact, got, vector.SHA256)
		}
	}
	for _, id := range []string{"bucket-truncate", "timestamp-pruning", "field-id", "row-ordinal"} {
		if !seen[id] {
			t.Fatalf("missing required golden vector provenance id %s", id)
		}
	}
}
