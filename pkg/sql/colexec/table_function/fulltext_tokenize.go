// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/monlp/tokenizer"
)

type FullTextEntry struct {
	DocId any
	Pos   int32
	Word  string
}

type Document struct {
	Words []FullTextEntry
}

type tokenizeState struct {
	inited bool
	called bool
	param  FullTextParserParam
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
		if len(tf.Params) > 0 {
			err := json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

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

	id := vector.GetAny(idVec, nthRow)
	c := contentVec.GetStringAt(nthRow)

	var doc Document

	switch u.param.Parser {
	case "", "ngram", "default":
		tok, _ := tokenizer.NewSimpleTokenizer([]byte(c))
		for t := range tok.Tokenize() {

			slen := t.TokenBytes[0]
			word := string(t.TokenBytes[1 : slen+1])

			doc.Words = append(doc.Words, FullTextEntry{DocId: id, Word: word, Pos: t.BytePos})
		}
	case "json":
		var bj bytejson.ByteJson
		if err := json.Unmarshal([]byte(c), &bj); err != nil {
			return err
		}

		for t := range bj.TokenizeValue(false) {
			slen := t.TokenBytes[0]
			word := string(t.TokenBytes[1 : slen+1])
			doc.Words = append(doc.Words, FullTextEntry{DocId: id, Word: word, Pos: t.TokenPos})
		}
	default:
		return moerr.NewInternalError(proc.Ctx, "Invalid fulltext parser")
	}

	// write the batch
	for i := range doc.Words {
		// type of id follow primary key column
		vector.AppendAny(u.batch.Vecs[0], doc.Words[i].DocId, false, proc.Mp())
		// pos
		vector.AppendFixed[int32](u.batch.Vecs[1], doc.Words[i].Pos, false, proc.Mp())
		// word
		vector.AppendBytes(u.batch.Vecs[2], []byte(doc.Words[i].Word), false, proc.Mp())
	}

	u.batch.SetRowCount(len(doc.Words))
	return nil
}
