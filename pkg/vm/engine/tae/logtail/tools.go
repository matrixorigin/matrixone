// Copyright 2021 Matrix Origin
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

package logtail

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap/zapcore"
)

func ToStringTemplate(vec containers.Vector, printN int, opts ...common.TypePrintOpt) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", vec.Length()))
	if printN < 0 || printN > vec.Length() {
		printN = vec.Length()
	}
	first := true
	typ := vec.GetType()
	for i := 0; i < printN; i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		v := vec.Get(i)
		vIsNull := vec.IsNull(i)
		_, _ = w.WriteString(common.TypeStringValue(*typ, v, vIsNull, opts...))
		first = false
	}

	return w.String()
}

func DebugBatchToString(name string, bat *containers.Batch, isSpecialRowID bool, lvl zapcore.Level) string {
	if logutil.GetSkip1Logger().Core().Enabled(lvl) {
		return BatchToString(name, bat, isSpecialRowID)
	}
	return "not required level"
}

func BatchToString(name string, bat *containers.Batch, isSpecialRowID bool) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[BatchName=%s]\n", name))
	for i, vec := range bat.Vecs {
		_, _ = w.WriteString(fmt.Sprintf("(attr=%s)", bat.Attrs[i]))
		if bat.Attrs[i] == catalog.PhyAddrColumnName {
			if isSpecialRowID {
				_, _ = w.WriteString(ToStringTemplate(vec, common.DefaultMaxRowsToPrint, common.WithSpecialRowid{}))
			} else {
				_, _ = w.WriteString(ToStringTemplate(vec, common.DefaultMaxRowsToPrint))
			}
		} else {
			_, _ = w.WriteString(ToStringTemplate(vec, common.DefaultMaxRowsToPrint, common.WithDoNotPrintBin{}))
		}
		_ = w.WriteByte('\n')
	}
	return w.String()
}

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		catalog.PhyAddrColumnName,
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		catalog.AttrCommitTs,
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(
			attr,
			containers.MakeVector(typs[i], mp),
		)
	}
	return bat
}

// GetDataWindowForLogtail returns the batch according to the writeSchema.
// columns are sorted by seqnum and vacancy is filled with zero value
func DataChangeToLogtailBatch(src *containers.BatchWithVersion) *containers.Batch {
	seqnums := src.Seqnums
	if len(seqnums) != len(src.Vecs) {
		panic("unmatched seqnums length")
	}

	// sort by seqnum
	sort.Sort(src)

	bat := containers.NewBatchWithCapacity(int(src.NextSeqnum) + 2)
	if src.Deletes != nil {
		bat.Deletes = src.Deletes
	}

	i := len(src.Seqnums) - 1
	// move special column first, no abort column in logtail
	if src.Seqnums[i] != objectio.SEQNUM_ROWID || src.Seqnums[i-1] != objectio.SEQNUM_COMMITTS {
		panic(fmt.Sprintf("bad last seqnums %v", src.Seqnums))
	}
	bat.AddVector(src.Attrs[i], src.Vecs[i])
	bat.AddVector(src.Attrs[i-1], src.Vecs[i-1])

	for i, seqnum := range seqnums {
		if seqnum >= objectio.SEQNUM_UPPER {
			// two special column has been moved
			continue
		}
		for len(bat.Vecs) < 2+int(seqnum) {
			bat.AppendPlaceholder()
		}
		bat.AddVector(src.Attrs[i], src.Vecs[i].TryConvertConst())
	}
	return bat
}

func TombstoneChangeToLogtailBatch(src *containers.BatchWithVersion) *containers.Batch {
	seqnums := src.Seqnums
	if len(seqnums) != len(src.Vecs) {
		panic("unmatched seqnums length")
	}
	if len(src.Vecs) != 4 {
		panic(fmt.Sprintf("logic err, attr %v", src.Attrs))
	}

	bat := containers.NewBatchWithCapacity(3)

	// move special column first, no abort column in logtail
	if src.Seqnums[2] != objectio.SEQNUM_ROWID || src.Seqnums[3] != objectio.SEQNUM_COMMITTS {
		panic(fmt.Sprintf("bad last seqnums %v", src.Seqnums))
	}
	bat.AddVector(src.Attrs[0], src.Vecs[0]) // rowid
	bat.AddVector(src.Attrs[3], src.Vecs[3]) // committs
	bat.AddVector(src.Attrs[1], src.Vecs[1]) // pk
	bat.AddVector(src.Attrs[2], src.Vecs[2]) // PhyAddrColumn

	return bat
}

func containersBatchToProtoBatch(bat *containers.Batch) (*api.Batch, error) {
	mobat := containers.ToCNBatch(bat)
	return batch.BatchToProtoBatch(mobat)
}
