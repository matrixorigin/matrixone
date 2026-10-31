// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package postdml

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestFulltextPostDMLQuotesIdentifiers(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	bat := batch.New([]string{"key`id"})
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())
	op := PostDml{PostDmlCtx: &PostDmlCtx{
		Ref:           &plan.ObjectRef{SchemaName: "db`name"},
		PrimaryKeyIdx: 0, PrimaryKeyName: "key`id", IsDelete: true, IsInsert: true,
		FullText: &PostDmlFullTextCtx{SourceTableName: "table`name", IndexTableName: "index`name", Parts: []string{"tick`col", "order"}},
	}}
	require.NoError(t, op.runPostDml(proc, vm.CallResult{Batch: bat}))
	deletion, ok := proc.Base.PostDmlSqlList.Get(0)
	require.True(t, ok)
	require.Contains(t, deletion, "`db``name`.`index``name`")
	insertion, ok := proc.Base.PostDmlSqlList.Get(1)
	require.True(t, ok)
	require.Contains(t, insertion, "`db``name`.`table``name`")
	require.Contains(t, insertion, "`src`.`key``id`")
	require.Contains(t, insertion, "`src`.`tick``col`, `src`.`order`")
	for _, sql := range []string{deletion, insertion} {
		statements, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
	}
}
