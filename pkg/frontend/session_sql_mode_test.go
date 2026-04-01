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
	"sync/atomic"
	"testing"
)

// TestGetSqlModeNoAutoValueOnZero_CachedTrue tests when sqlModeNoAutoValueOnZero is cached as true
func TestGetSqlModeNoAutoValueOnZero_CachedTrue(t *testing.T) {
	ses := &Session{}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 1)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if !has {
		t.Errorf("Expected has to be true when cached value is 1, got %v", has)
	}
	if !ok {
		t.Errorf("Expected ok to be true when cached value is 1, got %v", ok)
	}
}

// TestGetSqlModeNoAutoValueOnZero_CachedFalse tests when sqlModeNoAutoValueOnZero is cached as false
func TestGetSqlModeNoAutoValueOnZero_CachedFalse(t *testing.T) {
	ses := &Session{}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 0)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if has {
		t.Errorf("Expected has to be false when cached value is 0, got %v", has)
	}
	if !ok {
		t.Errorf("Expected ok to be true when cached value is 0, got %v", ok)
	}
}

// TestGetSqlModeNoAutoValueOnZero_NilSesSysVars tests when sesSysVars is nil
func TestGetSqlModeNoAutoValueOnZero_NilSesSysVars(t *testing.T) {
	ses := &Session{}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if has {
		t.Errorf("Expected has to be false when sesSysVars is nil, got %v", has)
	}
	if ok {
		t.Errorf("Expected ok to be false when sesSysVars is nil, got %v", ok)
	}
}

// TestGetSqlModeNoAutoValueOnZero_HasNoAutoValueOnZero tests when sql_mode contains NO_AUTO_VALUE_ON_ZERO
func TestGetSqlModeNoAutoValueOnZero_HasNoAutoValueOnZero(t *testing.T) {
	ses := &Session{}
	ses.sesSysVars = &SystemVariables{
		mp: map[string]interface{}{
			"sql_mode": "NO_AUTO_VALUE_ON_ZERO",
		},
	}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if !has {
		t.Errorf("Expected has to be true when sql_mode contains NO_AUTO_VALUE_ON_ZERO, got %v", has)
	}
	if !ok {
		t.Errorf("Expected ok to be true when sql_mode contains NO_AUTO_VALUE_ON_ZERO, got %v", ok)
	}

	if atomic.LoadInt32(&ses.sqlModeNoAutoValueOnZero) != 1 {
		t.Errorf("Expected cached value to be 1 after parsing, got %v", atomic.LoadInt32(&ses.sqlModeNoAutoValueOnZero))
	}
}

// TestGetSqlModeNoAutoValueOnZero_NoNoAutoValueOnZero tests when sql_mode does not contain NO_AUTO_VALUE_ON_ZERO
func TestGetSqlModeNoAutoValueOnZero_NoNoAutoValueOnZero(t *testing.T) {
	ses := &Session{}
	ses.sesSysVars = &SystemVariables{
		mp: map[string]interface{}{
			"sql_mode": "STRICT_TRANS_TABLES",
		},
	}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if has {
		t.Errorf("Expected has to be false when sql_mode does not contain NO_AUTO_VALUE_ON_ZERO, got %v", has)
	}
	if !ok {
		t.Errorf("Expected ok to be true when sql_mode does not contain NO_AUTO_VALUE_ON_ZERO, got %v", ok)
	}

	if atomic.LoadInt32(&ses.sqlModeNoAutoValueOnZero) != 0 {
		t.Errorf("Expected cached value to be 0 after parsing, got %v", atomic.LoadInt32(&ses.sqlModeNoAutoValueOnZero))
	}
}

// TestGetSqlModeNoAutoValueOnZero_CaseInsensitive tests that NO_AUTO_VALUE_ON_ZERO is case insensitive
func TestGetSqlModeNoAutoValueOnZero_CaseInsensitive(t *testing.T) {
	testCases := []string{
		"no_auto_value_on_zero",
		"No_Auto_Value_On_Zero",
		"NO_AUTO_VALUE_ON_ZERO",
		"STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO",
		"NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ses := &Session{}
			ses.sesSysVars = &SystemVariables{
				mp: map[string]interface{}{
					"sql_mode": tc,
				},
			}
			atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

			has, ok := ses.GetSqlModeNoAutoValueOnZero()
			if !has {
				t.Errorf("Expected has to be true for sql_mode=%q, got %v", tc, has)
			}
			if !ok {
				t.Errorf("Expected ok to be true for sql_mode=%q, got %v", tc, ok)
			}
		})
	}
}

// TestGetSqlModeNoAutoValueOnZero_InvalidSqlMode tests when sql_mode is not a string
func TestGetSqlModeNoAutoValueOnZero_InvalidSqlMode(t *testing.T) {
	ses := &Session{}
	ses.sesSysVars = &SystemVariables{
		mp: map[string]interface{}{
			"sql_mode": 123,
		},
	}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	has, ok := ses.GetSqlModeNoAutoValueOnZero()
	if has {
		t.Errorf("Expected has to be false when sql_mode is not a string, got %v", has)
	}
	if ok {
		t.Errorf("Expected ok to be false when sql_mode is not a string, got %v", ok)
	}
}
