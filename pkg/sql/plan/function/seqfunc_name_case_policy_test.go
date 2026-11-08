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

package function

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Issue #31909: the string argument of nextval/currval/setval must resolve
// under the session's real lower_case_table_names policy, exactly like the
// identifiers of the CREATE SEQUENCE statement the parser normalized. Under
// the default policy CREATE SEQUENCE MySeq stores "myseq", so
// nextval('MySeq') must resolve that stored name instead of failing with
// "no such table". Case-sensitive mode 0 keeps the argument verbatim.

func sequenceCasePolicyResolver(mode int64) func(name string, isSystem, isGlobal bool) (interface{}, error) {
	return func(name string, isSystem, isGlobal bool) (interface{}, error) {
		if name == "lower_case_table_names" && isSystem && !isGlobal {
			return mode, nil
		}
		return nil, nil
	}
}

func TestResolveNextvalAppliesSessionCasePolicy(t *testing.T) {
	t.Run("mixed-case argument resolves the stored lowercase name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Base.TxnOperator = txn
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(1))

		eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
		db.EXPECT().Relation(gomock.Any(), "myseq", nil).Return(rel, nil)
		rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
			sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
		}, nil)
		db.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid")
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(314))

		id, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.NoError(t, err)
		require.Equal(t, "myseq", id.tblname,
			"the allocation identity must carry the normalized name that resolves the relation")
		require.Equal(t, "dbid", id.wantDB)
		require.Equal(t, uint64(314), id.wantRel)
		require.Equal(t, rel, id.rel)
	})

	t.Run("default policy lowercases when no session resolver is installed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Base.TxnOperator = txn
		// no resolver: production must fall back to the same default the
		// frontend parser uses (lower_case_table_names = 1)

		eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
		db.EXPECT().Relation(gomock.Any(), "myseq", nil).Return(rel, nil)
		rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
			sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
		}, nil)
		db.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid")
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(314))

		id, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.NoError(t, err)
		require.Equal(t, "myseq", id.tblname)
	})

	t.Run("case-sensitive mode 0 keeps the argument verbatim", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Base.TxnOperator = txn
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(0))

		eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
		// With case sensitivity the stored name IS "MySeq" (CREATE kept the
		// letters), so the argument must be handed over unchanged.
		db.EXPECT().Relation(gomock.Any(), "MySeq", nil).Return(rel, nil)
		rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
			sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
		}, nil)
		db.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid")
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(314))

		id, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.NoError(t, err)
		require.Equal(t, "MySeq", id.tblname)
	})

	t.Run("mixed mode 2 reaches the lookup with the created spelling", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Base.TxnOperator = txn
		// The mysql lexer folds CREATE identifiers only under mode 1
		// (GetDbOrTblName), so CREATE SEQUENCE MySeq in mode 2 stores
		// "MySeq" and the function argument must match that spelling.
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(2))

		eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
		db.EXPECT().Relation(gomock.Any(), "MySeq", nil).Return(rel, nil)
		rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
			sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
		}, nil)
		db.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid")
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(314))

		id, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.NoError(t, err)
		require.Equal(t, "MySeq", id.tblname,
			"mode 2 must carry the created spelling into the relation lookup and identity")
	})

	t.Run("mode 2 lookup failure recovers on the next call", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Base.TxnOperator = txn
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(2))

		eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil).Times(2)
		// First lookup fails; the failed attempt must not remap or cache the
		// name, so the retry asks for the same authored spelling again.
		db.EXPECT().Relation(gomock.Any(), "MySeq", nil).Return(nil, moerr.NewNoSuchTable(proc.Ctx, "db", "MySeq")).Times(1)
		_, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.Error(t, err)

		db.EXPECT().Relation(gomock.Any(), "MySeq", nil).Return(rel, nil).Times(1)
		rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
			sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
		}, nil)
		db.EXPECT().GetDatabaseId(gomock.Any()).Return("dbid")
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(314))

		id, err := resolveNextval("MySeq", "db", proc, eng, txn)
		require.NoError(t, err)
		require.Equal(t, "MySeq", id.tblname)
	})
}

// SETVAL resolves through the same boundary: the normalization decision is
// visible at the relation lookup, before any allocation SQL runs.
func TestSetvalResolvesUnderSessionCasePolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.TxnOperator = txn
	proc.SetResolveVariableFunc(sequenceCasePolicyResolver(1))

	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
	// The mixed-case argument must reach the engine as the stored lowercase
	// name; the ordinary-table kind check then fires before any SQL.
	db.EXPECT().Relation(gomock.Any(), "myseq", nil).Return(rel, nil)
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
	}, nil)

	_, err := setval("MySeq", "20", true, "db", proc, txn, eng)
	require.EqualError(t, err, "internal error: Table input is not a sequence")
}

// Under the mixed policy the stored spelling is "MySeq" (CREATE folds
// identifiers only in mode 1), so the SETVAL boundary must hand the authored
// spelling to the relation lookup unchanged.
func TestSetvalKeepsMode2CreatedSpelling(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.TxnOperator = txn
	proc.SetResolveVariableFunc(sequenceCasePolicyResolver(2))

	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
	// The created spelling must reach the engine unchanged; the
	// ordinary-table kind check then fires before any allocation SQL.
	db.EXPECT().Relation(gomock.Any(), "MySeq", nil).Return(rel, nil)
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
	}, nil)

	_, err := setval("MySeq", "20", true, "db", proc, txn, eng)
	require.EqualError(t, err, "internal error: Table input is not a sequence")
}
