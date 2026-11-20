// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/indexstats/sqlreader"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type indexStatsState struct {
	config indexstats.Config
	rows   indexstats.Rows
	next   int
	batch  *batch.Batch
}

func indexStatsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	state := &indexStatsState{}
	if len(tf.Args) != 3 && len(tf.Args) != 4 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "mo_index_stats requires 3 or 4 arguments (database, table, index [, options])")
	}
	if err := json.Unmarshal(tf.Params, &state.config); err != nil {
		return nil, err
	}
	if (len(tf.Args) == 4) != (state.config.Analyze != nil) {
		return nil, moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: invalid planned options")
	}
	return state, nil
}

func (s *indexStatsState) reset(_ *TableFunction, _ *process.Process) {
	s.rows = indexstats.Rows{}
	s.next = 0
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
}

func (s *indexStatsState) free(tf *TableFunction, proc *process.Process, _ bool, _ error) {
	s.reset(tf, proc)
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
}

func (s *indexStatsState) end(_ *TableFunction, _ *process.Process) error { return nil }

func (s *indexStatsState) start(tf *TableFunction, proc *process.Process, _ int, _ process.Analyzer) (err error) {
	s.reset(tf, proc)
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	}
	sp := sqlexec.NewSqlProcess(proc)
	sp.ApplyScanSnapshot(tf.ScanSnapshot)
	if s.config.KeyKind != "" {
		s.rows, err = sqlreader.ReadKey(sp, s.config)
		return err
	}
	owner, err := indexplugin.IndexStats(proc.Ctx, s.config.Algorithm)
	if err != nil {
		return err
	}
	s.rows, err = owner.ReadIndexStats(sp, s.config)
	return err
}

func (s *indexStatsState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.next >= s.rows.Count {
		return vm.CancelResult, nil
	}
	s.batch.CleanOnlyData()
	count := 0
	for count < 128 && s.next < s.rows.Count {
		row := s.rows.At(s.next)
		for i, attr := range tf.Attrs {
			v := s.batch.Vecs[i]
			var err error
			switch strings.ToLower(attr) {
			case "index_type":
				err = vector.AppendBytes(v, []byte(s.rows.IndexType), false, proc.Mp())
			case "row_type":
				err = vector.AppendBytes(v, []byte(row.Kind), false, proc.Mp())
			case "object_id":
				err = vector.AppendBytes(v, []byte(row.ObjectID), row.ObjectID == "", proc.Mp())
			case "metrics":
				encoded, encodeErr := json.Marshal(row.Metrics)
				if encodeErr != nil {
					return vm.CancelResult, encodeErr
				}
				value, parseErr := types.ParseSliceToByteJson(encoded)
				if parseErr != nil {
					return vm.CancelResult, parseErr
				}
				err = vector.AppendByteJson(v, value, false, proc.Mp())
			default:
				err = moerr.NewInvalidInputf(proc.Ctx, "mo_index_stats: unknown output column %s", attr)
			}
			if err != nil {
				return vm.CancelResult, err
			}
		}
		s.next++
		count++
	}
	s.batch.SetRowCount(count)
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}
