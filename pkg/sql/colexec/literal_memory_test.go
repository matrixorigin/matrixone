// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package colexec_test

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLiteralExecutorFailureReleasesPartialVector(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	baseline := proc.Mp().AllocatedBytes()
	oldLimit := mpool.CapLimit
	mpool.CapLimit = 1 << 20
	t.Cleanup(func() { mpool.CapLimit = oldLimit })
	for _, typ := range []types.T{types.T_varchar, types.T_varbinary, types.T_array_float32} {
		value := strings.Repeat("x", 2<<20)
		if typ == types.T_array_float32 {
			// This variant allocates a constant header before binary array
			// storage fails, and shares the executor's constructor-error path.
			value = strings.Repeat("\x00", 2<<20)
		}
		literal := &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}
		if typ == types.T_array_float32 {
			literal.Value = &plan.Literal_VecVal{VecVal: value}
		}
		expr := &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Lit{Lit: literal}}
		exec, err := colexec.NewExpressionExecutor(proc, expr)
		require.Error(t, err)
		require.Nil(t, exec)
		require.Equal(t, baseline, proc.Mp().AllocatedBytes(), "all constructor variants must retire partial native/Go vector storage")
	}
}
