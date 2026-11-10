// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnif

import (
	"bytes"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/stretchr/testify/require"
)

func newLogicalMemo() *TxnMemo {
	memo := NewTxnMemo()
	memo.AddTable(1, 10)
	memo.AddTable(2, 20)
	return memo
}

func TestLogicalDMLMemo(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(map[bool]string{false: "known empty", true: "logical subset"}[marked], func(t *testing.T) {
			memo := newLogicalMemo()
			require.False(t, memo.LogicalDMLTrackingEnabled())
			require.False(t, memo.LogicalDMLKnown())
			require.Error(t, memo.RecordLogicalDML(1, 10))
			var historical bytes.Buffer
			_, err := memo.WriteTo(&historical)
			require.NoError(t, err)
			memo.EnableLogicalDMLTracking()
			if marked {
				require.NoError(t, memo.RecordLogicalDML(1, 10))
				require.NoError(t, memo.RecordLogicalDML(1, 10))
			}
			require.Error(t, memo.RecordLogicalDML(2, 10))
			require.Error(t, memo.RecordLogicalDML(1, 99))
			var extension bytes.Buffer
			n, err := memo.WriteLogicalDMLTo(&extension)
			require.NoError(t, err)
			require.Equal(t, memo.LogicalDMLSize(), n)
			decoded := NewTxnMemo()
			_, err = decoded.ReadFromWithVersion(&historical, model.MemoTreeVersion4)
			require.NoError(t, err)
			require.False(t, decoded.LogicalDMLKnown(), "legacy layout must not imply no writes")
			_, err = decoded.ReadLogicalDMLFrom(&extension)
			require.NoError(t, err)
			require.True(t, decoded.LogicalDMLKnown())
			require.Equal(t, memo.LogicalDMLTables(), decoded.LogicalDMLTables())
			require.Equal(t, 2, decoded.TableCount())
			decoded.Reset()
			require.True(t, decoded.LogicalDMLTrackingEnabled())
			require.True(t, decoded.LogicalDMLKnown())
			require.Empty(t, decoded.LogicalDMLTables())
			require.Zero(t, decoded.TableCount())
		})
	}
	t.Run("legacy re-encoding remains unknown", func(t *testing.T) {
		var old bytes.Buffer
		_, err := newLogicalMemo().WriteTo(&old)
		require.NoError(t, err)
		memo := NewTxnMemo()
		_, err = memo.ReadFromWithVersion(&old, model.MemoTreeVersion4)
		require.NoError(t, err)
		memo.EnableLogicalDMLTracking()
		require.Error(t, memo.RecordLogicalDML(1, 10))
		var extension bytes.Buffer
		_, err = memo.WriteLogicalDMLTo(&extension)
		require.NoError(t, err)
		decoded := newLogicalMemo()
		_, err = decoded.ReadLogicalDMLFrom(&extension)
		require.NoError(t, err)
		require.False(t, decoded.LogicalDMLKnown())
		require.Empty(t, decoded.LogicalDMLTables())
	})
}

func TestLogicalDMLMemoRejectsMalformed(t *testing.T) {
	valid := []byte{1, 1, 0, 0, 0}
	db, table := uint64(1), uint64(10)
	valid = append(valid, types.EncodeUint64(&db)...)
	valid = append(valid, types.EncodeUint64(&table)...)
	variants := map[string][]byte{
		"bad flag": {2, 0, 0, 0, 0}, "oversized count": {1, 3, 0, 0, 0},
		"unknown with rows": append([]byte{0, 1, 0, 0, 0}, valid[5:]...),
		"duplicate":         append(append([]byte{1, 2, 0, 0, 0}, valid[5:]...), valid[5:]...),
		"wrong database":    append([]byte(nil), valid...),
		"wrong table":       append([]byte(nil), valid...),
	}
	variants["wrong database"][5] = 2
	variants["wrong table"][13] = 99
	for i := 0; i < len(valid); i++ {
		variants[string(rune('a'+i))+" truncated"] = valid[:i]
	}
	for name, data := range variants {
		t.Run(name, func(t *testing.T) {
			memo := newLogicalMemo()
			memo.EnableLogicalDMLTracking()
			require.NoError(t, memo.RecordLogicalDML(2, 20))
			_, err := memo.ReadLogicalDMLFrom(bytes.NewReader(data))
			require.Error(t, err)
			require.False(t, memo.LogicalDMLKnown())
			require.False(t, memo.LogicalDMLTrackingEnabled())
		})
	}
	t.Run("writer detects short output", func(t *testing.T) {
		memo := newLogicalMemo()
		memo.EnableLogicalDMLTracking()
		_, err := memo.WriteLogicalDMLTo(logicalMemoShortWriter{})
		require.ErrorIs(t, err, io.ErrShortWrite)
	})
	t.Run("legacy parser reuse clears newer tags", func(t *testing.T) {
		memo := newLogicalMemo()
		memo.EnableLogicalDMLTracking()
		require.NoError(t, memo.RecordLogicalDML(2, 20))
		memo.AddCatalogChange()
		var old bytes.Buffer
		_, err := NewTxnMemo().WriteTo(&old)
		require.NoError(t, err)
		_, err = memo.ReadFromWithVersion(&old, model.MemoTreeVersion4)
		require.NoError(t, err)
		require.Zero(t, memo.TableCount())
		require.Empty(t, memo.LogicalDMLTables())
		require.False(t, memo.LogicalDMLKnown())
		require.False(t, memo.HasCatalogChanges())
	})
}

type logicalMemoShortWriter struct{}

func (logicalMemoShortWriter) Write(p []byte) (int, error) { return 0, nil }
