//go:build mo_m0

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

package catalog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func m0ProofBase(t *testing.T) *Schema {
	t.Helper()
	_, s := m0CustomerLayout(t)
	// Rebuild a version-zero CREATE layout using the six original columns and
	// the hidden composite PK. No lineage or historical-schema witness is kept.
	s.ColDefs = append(s.ColDefs[:7], s.ColDefs[8])
	s.Extra.NextColSeqnum = 7
	s.Version = 0
	m0Reindex(t, s)
	return s
}

func m0Reindex(t *testing.T, s *Schema) {
	t.Helper()
	s.NameMap = make(map[string]int)
	s.SeqnumMap = make(map[uint16]int)
	for i, col := range s.ColDefs {
		col.Idx = i
		if !col.IsPhyAddr() {
			col.SeqNum = uint16(i)
		}
		s.NameMap[col.Name] = i
	}
	require.NoError(t, s.Finalize(true))
}

func m0ProofAdd(t *testing.T, s *Schema, name string, typ plan.Type) *Schema {
	t.Helper()
	s = s.Clone()
	require.NoError(t, s.ApplyAlterTable(api.NewAddColumnReq(0, 0, name, typ, -1)))
	s.Version++ // one successful independent TN ALTER transaction
	return s
}

func m0WriteProofObject(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, s *Schema, version uint32, skip int) string {
	t.Helper()
	var seqs []uint16
	var cols []*ColDef
	for _, col := range s.ColDefs {
		if !col.IsPhyAddr() && col.Idx != skip {
			seqs = append(seqs, col.SeqNum)
			cols = append(cols, col)
		}
	}
	bat := batch.NewWithSize(len(cols))
	defer bat.Clean(mp)
	for i, col := range cols {
		v := vector.NewVec(col.Type)
		bat.SetVector(int32(i), v)
		for row := 0; row < 3; row++ {
			switch col.Type.Oid {
			case types.T_int32:
				require.NoError(t, vector.AppendFixed(v, int32(row+1), false, mp))
			case types.T_decimal64:
				require.NoError(t, vector.AppendFixed(v, types.Decimal64(5000000), false, mp))
			case types.T_varchar:
				require.NoError(t, vector.AppendBytes(v, []byte{byte('a' + row)}, false, mp))
			default:
				t.Fatalf("unsupported test type %s", col.Type)
			}
		}
	}
	bat.SetRowCount(3)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	w, err := objectio.NewObjectWriter(name, fs, version, seqs, nil)
	require.NoError(t, err)
	w.SetSortKeySeqnum(s.GetPrimaryKey().SeqNum)
	_, err = w.Write(bat)
	require.NoError(t, err)
	_, err = w.WriteEnd(context.Background())
	require.NoError(t, err)
	return name.String()
}

func m0ReadProofObject(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, name string) (objectio.Header, objectio.ObjectDataMeta) {
	t.Helper()
	r, err := objectio.NewObjectReaderWithStr(name, fs)
	require.NoError(t, err)
	h, err := r.ReadHeader(context.Background(), mp)
	require.NoError(t, err)
	meta, err := r.ReadAllMeta(context.Background(), mp)
	require.NoError(t, err)
	return h, meta.MustDataMeta()
}

// This is a persisted-format feasibility test, not a SQL clone or database
// WAL/checkpoint test. It deliberately removes all saved parent/schema inputs
// before reopening the child schema and physical object files.
func TestM0AppendProofPersistedWithoutParentHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := fileservice.Config{Name: defines.SharedFileServiceName, Backend: "DISK-V2", DataDir: filepath.Join(dir, "objects"), Cache: fileservice.DisabledCacheConfig}
	fs, err := fileservice.NewFileService(ctx, cfg, nil)
	require.NoError(t, err)
	defer func() {
		if fs != nil {
			fs.Close(ctx)
		}
	}()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	base := m0ProofBase(t)
	oldName := m0WriteProofObject(t, fs, mp, base, 0, -1)
	parent := m0ProofAdd(t, base, "loyalty_tier", plan.Type{Id: int32(types.T_varchar), Width: 8})
	parentName := m0WriteProofObject(t, fs, mp, parent, 1, -1)
	child := m0ProofAdd(t, parent.Clone(), "credit_lim", plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2})
	newName := m0WriteProofObject(t, fs, mp, child, 2, -1)
	for name, schema := range map[string]*Schema{"parent": parent, "snapshot": parent, "child": child} {
		buf, err := schema.Marshal()
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), buf, 0600))
	}
	require.NoError(t, os.Remove(filepath.Join(dir, "parent")))
	require.NoError(t, os.Remove(filepath.Join(dir, "snapshot")))
	parent = nil
	fs.Close(ctx)
	fs, err = fileservice.NewFileService(ctx, cfg, nil)
	require.NoError(t, err)
	encoded, err := os.ReadFile(filepath.Join(dir, "child"))
	require.NoError(t, err)
	child = NewEmptySchema("customer")
	_, err = child.ReadFromWithVersion(bytes.NewReader(encoded), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	for _, test := range []struct {
		name string
		seq  uint16
		null bool
	}{
		{oldName, 7, true}, {oldName, 8, true},
		{parentName, 7, false}, {parentName, 8, true},
		{newName, 7, false}, {newName, 8, false},
	} {
		h, meta := m0ReadProofObject(t, fs, mp, test.name)
		require.Zero(t, meta.BlockHeader().TableID()) // no origin-ID dependency
		got, err := M0ProveAbsentColumn(child, h, meta, test.seq)
		require.NoError(t, err)
		require.Equal(t, test.null, got)
	}
}

func TestM0AppendProofRejectsMissingExpectedColumnsAndForeignDomain(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	base := m0ProofBase(t)
	a := m0ProofAdd(t, base, "a", plan.Type{Id: int32(types.T_varchar), Width: 8})
	b := m0ProofAdd(t, base, "b", plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2})
	require.Equal(t, a.Version, b.Version)
	require.Error(t, ValidateM0CloneLayout(m0PlanFromSchema(a), a, m0PlanFromSchema(b), b))
	sameType := m0ProofAdd(t, base, "different_name", plan.Type{Id: int32(types.T_varchar), Width: 8})
	require.Error(t, ValidateM0CloneLayout(m0PlanFromSchema(a), a, m0PlanFromSchema(sameType), sameType))
	reset := a.Clone()
	reset.Version = 0
	require.Error(t, ValidateM0CloneLayout(m0PlanFromSchema(a), a, m0PlanFromSchema(reset), reset))
	// No schema is resolved from this shared version number.
	name := m0WriteProofObject(t, fs, mp, a, 1, -1)
	h, meta := m0ReadProofObject(t, fs, mp, name)
	_, err = M0ProveAbsentColumn(b, h, meta, 7)
	require.Error(t, err) // a present column has the other branch's type
	// The declared version requires a, but the physical object omits it.
	name = m0WriteProofObject(t, fs, mp, a, 1, 7)
	h, meta = m0ReadProofObject(t, fs, mp, name)
	_, err = M0ProveAbsentColumn(a, h, meta, 7)
	require.Error(t, err)
	// Even with the correct MaxSeqnum, a hole in an expected base column fails.
	name = m0WriteProofObject(t, fs, mp, a, 1, 3)
	h, meta = m0ReadProofObject(t, fs, mp, name)
	require.Equal(t, uint16(7), meta.BlockHeader().MaxSeqnum())
	_, err = M0ProveAbsentColumn(a, h, meta, 7)
	require.Error(t, err)
	// Unrelated INPLACE version increments do not enter this domain.
	other := a.Clone()
	other.Version++
	_, err = M0AppendBase(other)
	require.Error(t, err)
}

func TestM0AppendProofCOPYStartsNewDomain(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	base := m0ProofBase(t)
	before := m0ProofAdd(t, base, "x", plan.Type{Id: int32(types.T_int32)})
	oldName := m0WriteProofObject(t, fs, mp, base, 0, -1)
	after := before.Clone()
	after.ColDefs[6], after.ColDefs[7] = after.ColDefs[7], after.ColDefs[6]
	after.Version = 0
	m0Reindex(t, after) // CREATE/COPY reallocates; objects must be rewritten
	after = m0ProofAdd(t, after, "z", plan.Type{Id: int32(types.T_int32)})
	h, meta := m0ReadProofObject(t, fs, mp, oldName)
	_, err = M0ProveAbsentColumn(after, h, meta, 8)
	require.Error(t, err) // never apply the new mapping to the old shared object
	copyBase := after.Clone()
	copyBase.ColDefs = append(copyBase.ColDefs[:8], copyBase.ColDefs[9])
	copyBase.Version = 0
	copyBase.Extra.NextColSeqnum = 8
	m0Reindex(t, copyBase)
	newName := m0WriteProofObject(t, fs, mp, copyBase, 0, -1)
	h, meta = m0ReadProofObject(t, fs, mp, newName)
	absent, err := M0ProveAbsentColumn(after, h, meta, 8)
	require.NoError(t, err)
	require.True(t, absent)
}
