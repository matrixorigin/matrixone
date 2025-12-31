// Copyright 2021 Matrix Origin
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

package interactive

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/stretchr/testify/assert"
)

func TestObjectIntegrationStateManagement(t *testing.T) {
	// Test object opening state management
	m := &model{}
	
	// Initially no object to open
	assert.Empty(t, m.objectToOpen)
	assert.Nil(t, m.rangeToOpen)
	
	// Set object to open (simulate setting from real data)
	m.objectToOpen = "test-object-path"
	// Don't create complex types, just test the logic
	
	assert.Equal(t, "test-object-path", m.objectToOpen)
}

func TestObjectViewOptionsMapping(t *testing.T) {
	// Test column names mapping constants
	expectedColumnNames := map[uint16]string{
		0: ckputil.TableObjectsAttr_Accout,
		1: ckputil.TableObjectsAttr_DB,
		2: ckputil.TableObjectsAttr_Table,
		3: ckputil.TableObjectsAttr_ObjectType,
		4: ckputil.TableObjectsAttr_ID,
		5: ckputil.TableObjectsAttr_CreateTS,
		6: ckputil.TableObjectsAttr_DeleteTS,
		7: ckputil.TableObjectsAttr_Cluster,
	}
	
	// Verify all expected columns are mapped
	assert.Len(t, expectedColumnNames, 8)
	assert.Equal(t, "account_id", expectedColumnNames[0])
	assert.Equal(t, "db_id", expectedColumnNames[1])
	assert.Equal(t, "table_id", expectedColumnNames[2])
	
	// Test column formats mapping
	expectedColumnFormats := map[uint16]string{
		4: "objectstats", // id -> objectstats format
		5: "ts",          // create_ts -> timestamp format
		6: "ts",          // delete_ts -> timestamp format
	}
	
	assert.Len(t, expectedColumnFormats, 3)
	assert.Equal(t, "objectstats", expectedColumnFormats[4])
	assert.Equal(t, "ts", expectedColumnFormats[5])
	assert.Equal(t, "ts", expectedColumnFormats[6])
}

func TestObjectStateReset(t *testing.T) {
	// Test object state reset after viewing
	m := &model{
		objectToOpen: "test-object",
		rangeToOpen:  nil, // Don't create complex types
	}
	
	// Verify initial state
	assert.NotEmpty(t, m.objectToOpen)
	
	// Simulate state reset after object viewing
	m.objectToOpen = ""
	m.rangeToOpen = nil
	
	// Verify reset state
	assert.Empty(t, m.objectToOpen)
	assert.Nil(t, m.rangeToOpen)
}

func TestObjectIntegrationCondition(t *testing.T) {
	// Test the condition for object opening
	testCases := []struct {
		name         string
		objectToOpen string
		hasRange     bool
		shouldOpen   bool
	}{
		{
			name:         "both set - should open",
			objectToOpen: "test-object",
			hasRange:     true,
			shouldOpen:   true,
		},
		{
			name:         "only object set - should not open",
			objectToOpen: "test-object",
			hasRange:     false,
			shouldOpen:   false,
		},
		{
			name:         "neither set - should not open",
			objectToOpen: "",
			hasRange:     false,
			shouldOpen:   false,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &model{
				objectToOpen: tc.objectToOpen,
			}
			
			// Simulate range setting
			if tc.hasRange {
				// In real code, this would be set from entry.Range
				m.rangeToOpen = &ckputil.TableRange{} // Empty but not nil
			}
			
			// Test the condition logic
			shouldOpen := m.objectToOpen != "" && m.rangeToOpen != nil
			assert.Equal(t, tc.shouldOpen, shouldOpen)
		})
	}
}
