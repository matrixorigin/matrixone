// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"os"
	"testing"
)

func TestResolveReclamationMode(t *testing.T) {
	old, had := os.LookupEnv(MemoryPolicyEnv)
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(MemoryPolicyEnv, old)
		} else {
			_ = os.Unsetenv(MemoryPolicyEnv)
		}
	})
	_ = os.Unsetenv(MemoryPolicyEnv)

	for _, tc := range []struct {
		name    string
		config  string
		env     string
		want    ReclamationMode
		wantErr bool
	}{
		{name: "default", want: AccountingAndReclamation},
		{name: "explicit accounting only", config: string(AccountingOnly), want: AccountingOnly},
		{name: "explicit reclamation", config: string(AccountingAndReclamation), want: AccountingAndReclamation},
		{name: "environment override", env: string(AccountingOnly), want: AccountingOnly},
		{name: "config wins", config: string(AccountingAndReclamation), env: string(AccountingOnly), want: AccountingAndReclamation},
		{name: "invalid", config: "unknown", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env == "" {
				_ = os.Unsetenv(MemoryPolicyEnv)
			} else {
				_ = os.Setenv(MemoryPolicyEnv, tc.env)
			}
			got, err := ResolveReclamationMode(tc.config)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an invalid policy error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve policy: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolved policy %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReclamationModeEnablesReclamation(t *testing.T) {
	if AccountingOnly.EnablesReclamation() {
		t.Fatal("accounting-only must not enable reclamation")
	}
	if !AccountingAndReclamation.EnablesReclamation() {
		t.Fatal("accounting+reclamation must enable reclamation")
	}
}
