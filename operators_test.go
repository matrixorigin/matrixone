package main

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestGenerateFilterExprOperators(t *testing.T) {
	m := mpool.MustNewNoFixed()
	proc := testutil.NewProcessWithMPool(m)

	type testCase struct {
		desc       []string
		exprs      []*plan.Expr
		expectVals []bool
	}

	tc := testCase{
		desc:       []string{},
		exprs:      []*plan.Expr{},
		expectVals: []bool{},
	}

	for i, _ := range tc.exprs {
		t.Log(i)
	}
}
