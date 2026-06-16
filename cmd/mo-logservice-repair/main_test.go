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
	"encoding/json"
	"strings"
	"testing"
)

func TestParseMembers(t *testing.T) {
	members, err := parseMembers(`{"262145":"log-1:32000","262147":"log-2:32000"}`)
	if err != nil {
		t.Fatalf("parse members: %v", err)
	}
	if members[262145] != "log-1:32000" {
		t.Fatalf("unexpected member 262145: %q", members[262145])
	}
	if members[262147] != "log-2:32000" {
		t.Fatalf("unexpected member 262147: %q", members[262147])
	}
}

func TestParseMembersRejectsInvalidInput(t *testing.T) {
	cases := []string{
		"",
		`{"bad":"log-1:32000"}`,
		`{"262145":""}`,
	}
	for _, c := range cases {
		if _, err := parseMembers(c); err == nil {
			t.Fatalf("expected error for %q", c)
		}
	}
}

func TestEncodeReason(t *testing.T) {
	if got := encodeReason("manual repair", nil); got != "manual repair" {
		t.Fatalf("unexpected plain reason: %q", got)
	}

	got := encodeReason("cleanup", map[string][]uint64{"store-d01": {262145}})
	if !strings.HasPrefix(got, repairReasonPrefix) {
		t.Fatalf("encoded reason missing prefix: %q", got)
	}
	var payload reasonPayload
	if err := json.Unmarshal([]byte(strings.TrimPrefix(got, repairReasonPrefix)), &payload); err != nil {
		t.Fatalf("unmarshal encoded reason: %v", err)
	}
	if payload.Reason != "cleanup" {
		t.Fatalf("unexpected reason: %q", payload.Reason)
	}
	if replicas := payload.CleanupReplicasByStore["store-d01"]; len(replicas) != 1 || replicas[0] != 262145 {
		t.Fatalf("unexpected cleanup replicas: %+v", payload.CleanupReplicasByStore)
	}
}

func TestSplitAddresses(t *testing.T) {
	got := splitAddresses(" log-0:32001, ,log-1:32001 ")
	if len(got) != 2 {
		t.Fatalf("unexpected address count: %v", got)
	}
	if got[0] != "log-0:32001" || got[1] != "log-1:32001" {
		t.Fatalf("unexpected addresses: %v", got)
	}
}
