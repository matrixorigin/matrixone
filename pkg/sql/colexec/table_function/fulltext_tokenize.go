package table_function

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/monlp/tokenizer"
)

type SeenIds struct {
	FirstDocId any
	LastDocId  any
}

type FullTextEntry struct {
	DocId    any
	Pos      int32
	Word     string
	DocCount int32
	SeenIds
}

type Document struct {
	Words []FullTextEntry
}

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

		logutil.Infof("TOKEN PARAMS %s", tf.Params)
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
	c := contentVec.GetStringAt(nthRow)

	var doc Document
	doc_count := make(map[string]int32)

	tok, _ := tokenizer.NewSimpleTokenizer([]byte(c))
	for t := range tok.Tokenize() {

		slen := t.TokenBytes[0]
		word := string(t.TokenBytes[1 : slen+1])

		if _, ok := doc_count[word]; ok {
			doc_count[word] += 1
		} else {
			doc_count[word] = 1
		}
		//doc.Words = append(doc.Words, FullTextEntry{DocId: id, Word: word, Pos: t.BytePos, SeenIds: SeenIds{FirstDocId: id, LastDocId: id}})
		doc.Words = append(doc.Words, FullTextEntry{DocId: id, Word: word, Pos: t.BytePos})
	}

	// update doc_count
	for i := range doc.Words {
		doc.Words[i].DocCount = doc_count[doc.Words[i].Word]
	}

	// write the batch
	for i := range doc.Words {

		// type of id follow primary key column
		vector.AppendAny(u.batch.Vecs[0], doc.Words[i].DocId, false, proc.Mp())
		// pos
		vector.AppendFixed[int32](u.batch.Vecs[1], doc.Words[i].Pos, false, proc.Mp())
		// word
		vector.AppendBytes(u.batch.Vecs[2], []byte(doc.Words[i].Word), false, proc.Mp())
		// doc_count
		vector.AppendFixed[int32](u.batch.Vecs[3], doc.Words[i].DocCount, false, proc.Mp())
		// first_doc_id
		vector.AppendAny(u.batch.Vecs[4], nil, true, proc.Mp())
		// last_doc_id
		vector.AppendAny(u.batch.Vecs[5], nil, true, proc.Mp())
	}

	u.batch.SetRowCount(len(doc.Words))
	return nil
}
