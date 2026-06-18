// Copyright 2024 Matrix Origin
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

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseLogConfigForWizard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log1.toml")
	data := `
[logservice]
uuid = "00000000-0000-0000-0000-000000000d01"
deployment-id = 8850055262063090202
data-dir = "/tmp/mo-zombie-restore-repro/data/log1/logservice-data"
raft-address = "127.0.0.1:65200"
gossip-address = "127.0.0.1:65202"
logservice-service-address = "127.0.0.1:65201"

[hakeeper-client]
service-addresses = ["127.0.0.1:65201", "127.0.0.1:65301"]
`
	if err := os.WriteFile(path, []byte(data), 0640); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := parseLogConfig(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.UUID != "00000000-0000-0000-0000-000000000d01" {
		t.Fatalf("unexpected uuid: %s", cfg.UUID)
	}
	if cfg.DeploymentID != 8850055262063090202 {
		t.Fatalf("unexpected deployment id: %d", cfg.DeploymentID)
	}
	if cfg.NodeHostDir != "/tmp/mo-zombie-restore-repro/data/log1/logservice-data/00000000-0000-0000-0000-000000000d01" {
		t.Fatalf("unexpected node host dir: %s", cfg.NodeHostDir)
	}
	if cfg.RaftAddress != "127.0.0.1:65200" || cfg.GossipAddress != "127.0.0.1:65202" {
		t.Fatalf("unexpected raft/gossip address: %s %s", cfg.RaftAddress, cfg.GossipAddress)
	}
	if len(cfg.HAKeeperAddrs) != 2 || cfg.HAKeeperAddrs[0] != "127.0.0.1:65201" {
		t.Fatalf("unexpected hakeeper addresses: %v", cfg.HAKeeperAddrs)
	}
}

func TestLocalShardReplicasScansTanNodes(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-262147"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-1-282826"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-2-123"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	replicas := localShardReplicas(dir, 8850055262063090202, 1)
	if len(replicas) != 2 || replicas[0] != 262147 || replicas[1] != 282826 {
		t.Fatalf("unexpected replicas: %v", replicas)
	}
}
