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

package frontend

import (
	"context"
	"testing"
)

// TestEnableRemapHintDisabled tests that rewriteSQL returns original SQL when enable_remap_hint is disabled
func TestEnableRemapHintDisabled(t *testing.T) {
	ses := &Session{
		cache: &privilegeCache{},
	}
	// Set rewriteEnabled to false
	ses.rewriteEnabled.Store(false)

	testSQL := "SELECT * FROM test_table"
	result, err := rewriteSQL(context.Background(), ses, testSQL)
	if err != nil {
		t.Errorf("rewriteSQL returned error when enable_remap_hint is disabled: %v", err)
	}

	if result != testSQL {
		t.Errorf("Expected original SQL when enable_remap_hint is disabled, got: %s", result)
	}
}

// TestEnableRemapHintEnabled tests that rewriteSQL may modify SQL when enable_remap_hint is enabled
func TestEnableRemapHintEnabled(t *testing.T) {
	ses := &Session{
		cache: &privilegeCache{},
	}
	// Set rewriteEnabled to true
	ses.rewriteEnabled.Store(true)
	// Empty rule cache, should return original SQL

	testSQL := "SELECT * FROM test_table"
	result, err := rewriteSQL(context.Background(), ses, testSQL)
	if err != nil {
		t.Errorf("rewriteSQL returned error when enable_remap_hint is enabled: %v", err)
	}

	// With empty rule cache, should return original SQL
	if result != testSQL {
		t.Errorf("Expected original SQL with empty rule cache, got: %s", result)
	}
}

// TestEnableRemapHintDefault tests that rewriteSQL uses default value when enable_remap_hint is not set
func TestEnableRemapHintDefault(t *testing.T) {
	ses := &Session{
		cache: &privilegeCache{},
		// rewriteEnabled is not set, should use default value (false)
	}

	testSQL := "SELECT * FROM test_table"
	result, err := rewriteSQL(context.Background(), ses, testSQL)
	if err != nil {
		t.Errorf("rewriteSQL returned error when enable_remap_hint is not set: %v", err)
	}

	// With rewriteEnabled not set (default false), should return original SQL
	if result != testSQL {
		t.Errorf("Expected original SQL with default rewriteEnabled, got: %s", result)
	}
}
