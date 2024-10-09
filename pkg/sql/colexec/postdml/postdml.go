// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package postdml

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "postdml"

var (
	fulltextInsertSqlFmt = "INSERT INTO %s SELECT f.* FROM %s CROSS APPLY fulltext_index_tokenize('%s', %s, %s) as f WHERE %s IN (%s)"
	fulltextDeleteSqlFmt = "DELETE FROM %s WHERE doc_id IN (%s)"
)

func (postdml *PostDml) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (postdml *PostDml) OpType() vm.OpType {
	return vm.PostDml
}

func (postdml *PostDml) Prepare(proc *process.Process) error {
	if postdml.OpAnalyzer == nil {
		postdml.OpAnalyzer = process.NewAnalyzer(postdml.GetIdx(), postdml.IsFirst, postdml.IsLast, "postdml")
	} else {
		postdml.OpAnalyzer.Reset()
	}

	ref := postdml.PostDmlCtx.Ref
	eng := postdml.PostDmlCtx.Engine
	partitionNames := postdml.PostDmlCtx.PartitionTableNames
	rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
	if err != nil {
		return err
	}

	postdml.ctr.source = rel
	postdml.ctr.partitionSources = partitionRels
	postdml.ctr.affectedRows = 0
	return nil
}

func (postdml *PostDml) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	if postdml.PostDmlCtx.FullText != nil {
		return postdml.runPostDmlFullText(proc)
	}

	return vm.CancelResult, nil
}

func (postdml *PostDml) runPostDmlFullText(proc *process.Process) (vm.CallResult, error) {
	analyzer := postdml.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ftctx := postdml.PostDmlCtx.FullText

	result, err := vm.ChildrenCall(postdml.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}

	var in_list []any

	if len(postdml.PostDmlCtx.PartitionTableIDs) > 0 {
		// partition tables

	} else {
		bat := result.Batch
		pkvec := bat.Vecs[ftctx.PkeyIdx]
		//pkTyp := pkvec.GetType()
		in_list = make([]any, 0, bat.RowCount())
		for i := 0; i < bat.RowCount(); i++ {
			pkey := vector.GetAny(pkvec, i)
			bytes, ok := pkey.([]byte)
			if ok {
				key := "'" + string(bytes) + "'"
				pkey = key
			}

			in_list = append(in_list, pkey)
		}
	}

	values := anyslice2str(in_list)
	if ftctx.IsDelete {
		// append Delete SQL
		sql := fmt.Sprintf(fulltextDeleteSqlFmt, ftctx.IndexTableName, values)

		logutil.Infof("DELETE SQL : %s", sql)
		proc.Base.PostDmlSqlList = append(proc.Base.PostDmlSqlList, sql)
	}

	if ftctx.IsInsert {
		sql := fmt.Sprintf(fulltextInsertSqlFmt, ftctx.IndexTableName, ftctx.SourceTableName, ftctx.AlgoParams, ftctx.PkeyName,
			strings.Join(ftctx.Parts, ", "), ftctx.SourceTableName, values)
		logutil.Infof("INSERT SQL : %s", sql)
		proc.Base.PostDmlSqlList = append(proc.Base.PostDmlSqlList, sql)

	}

	analyzer.Output(result.Batch)
	return result, nil
}

func anyslice2str(list []any) string {

	var s string
	for i, v := range list {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprint(v)
	}
	return s
}
