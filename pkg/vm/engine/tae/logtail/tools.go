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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
		objectio.TombstoneAttr_CommitTs_Attr,
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

	filterAbortedLogtailRows(src)

	// sort by seqnum
	sort.Sort(src)

	bat := containers.NewBatchWithCapacity(int(src.NextSeqnum) + 2)
	rowIDPos, commitTSPos := -1, -1
	for i, seqnum := range src.Seqnums {
		switch seqnum {
		case objectio.SEQNUM_ROWID:
			rowIDPos = i
		case objectio.SEQNUM_COMMITTS:
			commitTSPos = i
		}
	}
	// Abort is a storage-only column. Rolled-back rows were compacted above
	// and are never emitted in logtail.
	if rowIDPos == -1 || commitTSPos == -1 {
		panic(fmt.Sprintf("missing required logtail seqnums in %v", src.Seqnums))
	}
	bat.AddVector(src.Attrs[rowIDPos], src.Vecs[rowIDPos])
	bat.AddVector(src.Attrs[commitTSPos], src.Vecs[commitTSPos])

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
	filterAbortedLogtailRows(src)

	bat := containers.NewBatchWithCapacity(3)

	for _, attr := range []string{
		objectio.TombstoneAttr_Rowid_Attr,
		objectio.TombstoneAttr_CommitTs_Attr,
		objectio.TombstoneAttr_PK_Attr,
		catalog.PhyAddrColumnName,
	} {
		vec := src.GetVectorByName(attr)
		if vec == nil {
			panic(fmt.Sprintf("missing tombstone logtail column %q in %v", attr, src.Attrs))
		}
		bat.AddVector(attr, vec)
	}

	return bat
}

func filterAbortedLogtailRows(src *containers.BatchWithVersion) {
	abortPos, commitTSPos := -1, -1
	for i, seqnum := range src.Seqnums {
		switch seqnum {
		case objectio.SEQNUM_ABORT:
			abortPos = i
		case objectio.SEQNUM_COMMITTS:
			commitTSPos = i
		}
	}
	var aborts ioutil.TombstoneAbortColumn
	if abortPos != -1 {
		var err error
		aborts, err = ioutil.ValidateTombstoneAbortColumn(src.Length(), src.Vecs[abortPos].GetDownstreamVector())
		if err != nil {
			panic(err)
		}
	}
	var commitTSs []types.TS
	if commitTSPos != -1 && !src.Vecs[commitTSPos].IsConstNull() {
		commitTSs = vector.MustFixedColWithTypeCheck[types.TS](src.Vecs[commitTSPos].GetDownstreamVector())
	}
	for row := 0; row < src.Length(); row++ {
		if (aborts.IsPresent() && aborts.At(row)) ||
			(row < len(commitTSs) && commitTSs[row].Equal(&txnif.UncommitTS)) {
			src.Delete(row)
		}
	}
	if src.HasDelete() {
		src.Compact()
	}
}

func containersBatchToProtoBatch(bat *containers.Batch) (*api.Batch, error) {
	mobat := containers.ToCNBatch(bat)
	return batch.BatchToProtoBatch(mobat)
}
