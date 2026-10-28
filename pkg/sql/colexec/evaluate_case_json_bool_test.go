package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// captureSelectExecutor returns a fixed vector from every evaluation and
// records the row mask each call received, so a test can assert which rows a
// flow-control evaluator routed to which arm.
type captureSelectExecutor struct {
	result *vector.Vector
	seen   [][]bool
}

func (c *captureSelectExecutor) Eval(_ *process.Process, _ []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	c.seen = append(c.seen, append([]bool(nil), selectList...))
	return c.result, nil
}

func (c *captureSelectExecutor) EvalWithoutResultReusing(_ *process.Process, _ []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	c.seen = append(c.seen, append([]bool(nil), selectList...))
	return c.result, nil
}

func (c *captureSelectExecutor) ResetForNextQuery() {}

func (c *captureSelectExecutor) Free() {}

func (c *captureSelectExecutor) IsColumnExpr() bool { return false }

func (c *captureSelectExecutor) TypeName() string { return "capture" }

// TestEvalCaseJsonConditionInDomain pins the flow-control CASE evaluator on a
// JSON condition: the condition stays in its own domain (the BOOL parameter
// wrapper must not touch the JSON vector, which used to panic with a
// JSON-to-[]bool type assertion) and each arm is evaluated for exactly the
// rows whose MySQL conditional numeric truth selects it.
func TestEvalCaseJsonConditionInDomain(t *testing.T) {
	proc := testutil.NewProcess(t)

	encodeJSON := func(text string) []byte {
		bj, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		packed, err := bj.Marshal()
		require.NoError(t, err)
		return packed
	}

	conditions := []struct {
		name string
		data []byte
		null bool
	}{
		{name: `"2tail" numeric prefix is true`, data: encodeJSON(`"2tail"`)},
		{name: `"abc" is false`, data: encodeJSON(`"abc"`)},
		{name: `" 12" is true`, data: encodeJSON(`" 12"`)},
		{name: `string "true" is false`, data: encodeJSON(`"true"`)},
		{name: "sql null selects else", data: encodeJSON(`"1"`), null: true},
	}

	condVec := vector.NewVec(types.T_json.ToType())
	defer condVec.Free(proc.Mp())
	thenVec := vector.NewVec(types.T_int64.ToType())
	defer thenVec.Free(proc.Mp())
	elseVec := vector.NewVec(types.T_int64.ToType())
	defer elseVec.Free(proc.Mp())
	for i, cond := range conditions {
		require.NoError(t, vector.AppendBytes(condVec, cond.data, cond.null, proc.Mp()))
		require.NoError(t, vector.AppendFixed(thenVec, int64(10+i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(elseVec, int64(-1), false, proc.Mp()))
	}

	condExec := &captureSelectExecutor{result: condVec}
	thenExec := &captureSelectExecutor{result: thenVec}
	elseExec := &captureSelectExecutor{result: elseVec}
	expr := &FunctionExpressionExecutor{
		parameterResults:  make([]*vector.Vector, 3),
		parameterExecutor: []ExpressionExecutor{condExec, thenExec, elseExec},
	}
	expr.fid = function.CASE
	expr.resultType = types.T_int64.ToType()

	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = condVec
	input.SetRowCount(len(conditions))

	require.NotPanics(t, func() {
		require.NoError(t, expr.EvalCase(proc, []*batch.Batch{input}, nil))
	})

	require.Len(t, condExec.seen, 1)
	for row := range conditions {
		require.True(t, condExec.seen[0][row], "every row must reach the condition")
	}
	require.Len(t, thenExec.seen, 1, "the THEN arm must be evaluated exactly once")
	require.Equal(t, []bool{true, false, true, false, false}, thenExec.seen[0],
		"THEN must cover exactly the conditionally-true JSON rows")
	require.Len(t, elseExec.seen, 1, "the ELSE arm must be evaluated exactly once")
	require.Equal(t, []bool{false, true, false, true, true}, elseExec.seen[0],
		"ELSE must cover the false and SQL-NULL JSON rows")
}

// TestEvalCaseJsonConditionNeverDecodesJsonAsBoolInNativeMode pins the review
// repair (finding 5): a JSON condition vector only ever takes the in-domain
// conditional kernel, and a native-mode session - which can only receive
// such a condition from a cross-mode persisted expression - fails closed
// with the typed error instead of letting the fallback BOOL parameter
// decoding type-assert a T_json vector.
func TestEvalCaseJsonConditionNeverDecodesJsonAsBoolInNativeMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().MatrixOneNativeMode = true

	encodeJSON := func(text string) []byte {
		bj, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		packed, err := bj.Marshal()
		require.NoError(t, err)
		return packed
	}

	condVec := vector.NewVec(types.T_json.ToType())
	defer condVec.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(condVec, encodeJSON(`2`), false, proc.Mp()))
	thenVec := vector.NewVec(types.T_int64.ToType())
	defer thenVec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(thenVec, int64(1), false, proc.Mp()))
	elseVec := vector.NewVec(types.T_int64.ToType())
	defer elseVec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(elseVec, int64(0), false, proc.Mp()))

	condExec := &captureSelectExecutor{result: condVec}
	thenExec := &captureSelectExecutor{result: thenVec}
	elseExec := &captureSelectExecutor{result: elseVec}
	expr := &FunctionExpressionExecutor{
		parameterResults:  make([]*vector.Vector, 3),
		parameterExecutor: []ExpressionExecutor{condExec, thenExec, elseExec},
	}
	expr.fid = function.CASE
	expr.resultType = types.T_int64.ToType()

	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = condVec
	input.SetRowCount(1)

	require.NotPanics(t, func() {
		err := expr.EvalCase(proc, []*batch.Batch{input}, nil)
		require.Error(t, err,
			"a native-mode session must never evaluate a persisted JSON condition")
		require.ErrorContains(t, err, "MySQL conditional boolean contracts")
	})
	require.Empty(t, thenExec.seen, "the THEN arm must not be evaluated for a refused condition")
	require.Empty(t, elseExec.seen, "the ELSE arm must not be evaluated for a refused condition")
}
