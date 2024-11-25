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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	param  fulltext.FullTextParserParam
	doc    Document
	offset int
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func (u *tokenizeState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *tokenizeState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	// write the batch
	n := 0
	for i := u.offset; i < len(u.doc.Words) && n < 8192; i++ {
		// type of id follow primary key column
		vector.AppendAny(u.batch.Vecs[0], u.doc.Words[i].DocId, false, proc.Mp())
		// pos
		vector.AppendFixed[int32](u.batch.Vecs[1], u.doc.Words[i].Pos, false, proc.Mp())
		// word
		vector.AppendBytes(u.batch.Vecs[2], []byte(u.doc.Words[i].Word), false, proc.Mp())

		n++
	}

	u.offset += n
	u.batch.SetRowCount(n)

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
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

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *tokenizeState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		if len(tf.Params) > 0 {
			err := json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

		u.batch = tf.createResultBatch()
		u.doc = Document{Words: make([]FullTextEntry, 0, 512)}
		u.inited = true
	}

	// reset slice
	u.doc.Words = u.doc.Words[:0]
	u.offset = 0

	// cleanup the batch
	u.batch.CleanOnlyData()

	// tokenize
	vlen := len(tf.ctr.argVecs)

	idVec := tf.ctr.argVecs[0]
	id := vector.GetAny(idVec, nthRow)

	isnull := false
	for i := 1; i < vlen; i++ {
		isnull = (isnull || tf.ctr.argVecs[i].IsNull(uint64(nthRow)))
	}

	if isnull {
		return nil
	}

	switch u.param.Parser {
	case "", "ngram", "default":

		var c string
		for i := 1; i < vlen; i++ {
			if i > 1 {
				c += "\n"
			}
			data := tf.ctr.argVecs[i].GetStringAt(nthRow)

			// fix issue #19948 return vector type is not datalink even the input argurment type is datalink
			// so we have to check the input argument instead of vector
			//if tf.ctr.argVecs[i].GetType().Oid == types.T_datalink {
			if types.T(tf.Args[i].Typ.Id) == types.T_datalink {
				// datalink
				dl, err := datalink.NewDatalink(data, proc)
				if err != nil {
					return err
				}
				b, err := dl.GetPlainText(proc)
				if err != nil {
					return err
				}

				c += string(b)
			} else {
				c += data
			}
		}

		tok, _ := tokenizer.NewSimpleTokenizer([]byte(c))
		for t := range tok.Tokenize() {

			slen := t.TokenBytes[0]
			word := string(t.TokenBytes[1 : slen+1])

			u.doc.Words = append(u.doc.Words, FullTextEntry{DocId: id, Word: word, Pos: t.BytePos})
		}
	case "json":
		joffset := int32(0)
		for i := 1; i < vlen; i++ {
			c := tf.ctr.argVecs[i].GetRawBytesAt(nthRow)

			var bj bytejson.ByteJson
			//if tf.ctr.argVecs[i].GetType().Oid == types.T_json {
			if types.T(tf.Args[i].Typ.Id) == types.T_json {
				if err := bj.Unmarshal(c); err != nil {
					return err
				}
			} else {

				if err := json.Unmarshal(c, &bj); err != nil {
					return err
				}
			}

			voffset := int32(0)
			for t := range bj.TokenizeValue(false) {
				jslen := t.TokenBytes[0]
				value := string(t.TokenBytes[1 : jslen+1])
				// tokenize the value
				tok, _ := tokenizer.NewSimpleTokenizer([]byte(value))
				for tt := range tok.Tokenize() {
					tslen := tt.TokenBytes[0]
					word := string(tt.TokenBytes[1 : tslen+1])
					u.doc.Words = append(u.doc.Words, FullTextEntry{DocId: id, Word: word, Pos: joffset + voffset + tt.BytePos})
				}
				voffset += int32(jslen)
			}

			joffset += int32(len(c))
		}
	case "json_value":
		joffset := int32(0)
		for i := 1; i < vlen; i++ {
			c := tf.ctr.argVecs[i].GetRawBytesAt(nthRow)

			var bj bytejson.ByteJson
			//if tf.ctr.argVecs[i].GetType().Oid == types.T_json {
			if types.T(tf.Args[i].Typ.Id) == types.T_json {
				if err := bj.Unmarshal(c); err != nil {
					return err
				}
			} else {

				if err := json.Unmarshal(c, &bj); err != nil {
					return err
				}
			}

			voffset := int32(0)
			for t := range bj.TokenizeValue(false) {
				jslen := t.TokenBytes[0]
				value := string(t.TokenBytes[1 : jslen+1])
				u.doc.Words = append(u.doc.Words, FullTextEntry{DocId: id, Word: value, Pos: joffset + voffset})
				voffset += int32(jslen)
			}

			joffset += int32(len(c))
		}
	default:
		return moerr.NewInternalError(proc.Ctx, "Invalid fulltext parser")
	}

	return nil
}
