package table_function

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type tokenizeState struct {
	inited bool
	called bool
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func (u *tokenizeState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.called = false
}

func (u *tokenizeState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	var res vm.CallResult
	if u.called {
		return res, nil
	}
	res.Batch = u.batch
	u.called = true
	return res, nil
}

func (u *tokenizeState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func fulltextIndexTokenizePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &tokenizeState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *tokenizeState) start(tf *TableFunction, proc *process.Process, nthRow int) error {

	if !u.inited {

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.called = false
	// cleanup the batch
	u.batch.CleanOnlyData()

	// tokenize
	idVec := tf.ctr.argVecs[0]
	contentVec := tf.ctr.argVecs[1]

	if contentVec.IsNull(uint64(nthRow)) {
		u.batch.SetRowCount(0)
		return nil
	}

	var id any
	id = vector.GetAny(idVec, nthRow)
	//id := idVec.GetRawBytesAt(nthRow)
	c := contentVec.GetStringAt(nthRow)
	logutil.Infof("id = %d, content %s", id, c)

	// type of id follow primary key column
	vector.AppendAny(u.batch.Vecs[0], id, false, proc.Mp())
	// pos
	vector.AppendFixed[int32](u.batch.Vecs[1], 4, false, proc.Mp())
	// word
	vector.AppendBytes(u.batch.Vecs[2], []byte("red"), false, proc.Mp())
	// doc_count
	vector.AppendFixed[int32](u.batch.Vecs[3], 0, false, proc.Mp())
	// first_doc_id
	vector.AppendAny(u.batch.Vecs[4], id, false, proc.Mp())
	// last_doc_id
	vector.AppendAny(u.batch.Vecs[5], id, false, proc.Mp())

	u.batch.SetRowCount(1)
	return nil
}
