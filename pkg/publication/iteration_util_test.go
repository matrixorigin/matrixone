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

package publication

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
)

func TestGenerateSnapshotName(t *testing.T) {
	name := GenerateSnapshotName("task-123", 42)
	assert.Equal(t, "ccpr_task-123_42", name)
}

func TestDefaultSyncProtectionRetryOption(t *testing.T) {
	opt := DefaultSyncProtectionRetryOption()
	assert.Equal(t, 1*time.Second, opt.InitialInterval)
	assert.Equal(t, 5*time.Minute, opt.MaxTotalTime)
}

func TestTableKeyToString(t *testing.T) {
	key := TableKey{DBName: "mydb", TableName: "mytable"}
	assert.Equal(t, "mydb.mytable", tableKeyToString(key))
}

func TestParseTableKeyFromString_Valid(t *testing.T) {
	key := parseTableKeyFromString("mydb.mytable")
	assert.Equal(t, "mydb", key.DBName)
	assert.Equal(t, "mytable", key.TableName)
}

func TestParseTableKeyFromString_Legacy(t *testing.T) {
	key := parseTableKeyFromString("db_mydb")
	assert.Equal(t, "", key.DBName)
	assert.Equal(t, "", key.TableName)
}

func TestParseTableKeyFromString_Invalid(t *testing.T) {
	key := parseTableKeyFromString("nodot")
	assert.Equal(t, "", key.DBName)
	assert.Equal(t, "", key.TableName)
}

func TestParseTableKeyFromString_WithDotInTableName(t *testing.T) {
	key := parseTableKeyFromString("mydb.my.table")
	assert.Equal(t, "mydb", key.DBName)
	assert.Equal(t, "my.table", key.TableName)
}

func TestIsStale_Nil(t *testing.T) {
	assert.False(t, isStale(nil))
}

func TestIsStale_StaleMessage(t *testing.T) {
	meta := &ErrorMetadata{Message: "got stale read error"}
	assert.True(t, isStale(meta))
}

func TestIsStale_OtherMessage(t *testing.T) {
	meta := &ErrorMetadata{Message: "connection refused"}
	assert.False(t, isStale(meta))
}

func TestUpdateObjectStatsFlags(t *testing.T) {
	var stats objectio.ObjectStats

	// Test tombstone case: sorted should be true
	updateObjectStatsFlags(&stats, true, false)
	bytes := stats.Marshal()
	reserved := bytes[objectio.ObjectStatsLen-1]
	assert.True(t, reserved&objectio.ObjectFlag_Sorted != 0, "tombstone should be sorted")
	assert.True(t, reserved&objectio.ObjectFlag_CNCreated != 0, "should be CN created")

	// Test non-tombstone with no fake PK: sorted should be true
	var stats2 objectio.ObjectStats
	updateObjectStatsFlags(&stats2, false, false)
	bytes2 := stats2.Marshal()
	reserved2 := bytes2[objectio.ObjectStatsLen-1]
	assert.True(t, reserved2&objectio.ObjectFlag_Sorted != 0, "no fake PK should be sorted")

	// Test non-tombstone with fake PK: sorted should be false
	var stats3 objectio.ObjectStats
	updateObjectStatsFlags(&stats3, false, true)
	bytes3 := stats3.Marshal()
	reserved3 := bytes3[objectio.ObjectStatsLen-1]
	assert.True(t, reserved3&objectio.ObjectFlag_Sorted == 0, "fake PK should not be sorted")
	assert.True(t, reserved3&objectio.ObjectFlag_CNCreated != 0, "should be CN created")
}
