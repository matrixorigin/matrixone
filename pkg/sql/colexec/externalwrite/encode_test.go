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

package externalwrite

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func testBatch(t *testing.T, mp *mpool.MPool) *batch.Batch {
	bat := batch.New([]string{"id", "name"})

	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, true, mp)) // null
	bat.Vecs[0] = idVec

	nameVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(nameVec, []byte("alice"), false, mp))
	require.NoError(t, vector.AppendBytes(nameVec, []byte("bob"), false, mp))
	bat.Vecs[1] = nameVec

	bat.SetRowCount(2)
	return bat
}

func TestEncodeCSV(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format: FormatCSV,
		Attrs:  []string{"id", "name"},
		Stmt:   time.Now(),
	}).(*externalWriter)

	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "1,alice\n\\N,bob\n", string(out))
}

func TestEncodeJSONLine(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format: FormatJSONLine,
		Attrs:  []string{"id", "name"},
		Stmt:   time.Now(),
	}).(*externalWriter)

	out, err := w.encodeJSONLine(bat)
	require.NoError(t, err)
	require.Equal(t, "{\"id\":1,\"name\":\"alice\"}\n{\"id\":null,\"name\":\"bob\"}\n", string(out))
}
