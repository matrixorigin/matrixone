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
		v := vec.GetValue(i)
		vIsNull := vec.IsNull(uint64(i))
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
func DataChangeToLogtailBatch(src *containers.BatchWithVersion) (*containers.Batch, error) {
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

	// converted holds the flat copies made here, released on failure.
	var converted []containers.Vector
	for i, seqnum := range seqnums {
		if seqnum >= objectio.SEQNUM_UPPER {
			// two special column has been moved
			continue
		}
		for len(bat.Vecs) < 2+int(seqnum) {
			bat.AppendPlaceholder()
		}
		vec := src.Vecs[i]
		if vec.IsConst() {
			// logtail consumers expect flat vectors: expand a const one.
			mp := vec.MPool()
			if mp == nil {
				mp = common.LogtailAllocator
			}
			flat := vector.NewVec(*vec.GetType(), mp)
			if err := flat.UnionBatch(vec, 0, vec.Length(), nil, mp); err != nil {
				flat.Free(mp)
				for _, v := range converted {
					containers.ReleaseVector(v)
				}
				return nil, err
			}
			converted = append(converted, flat)
			vec = flat
		}
		bat.AddVector(src.Attrs[i], vec)
	}
	return bat, nil
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
	var aborts []bool
	if abortPos != -1 && !src.Vecs[abortPos].IsConstNull() {
		aborts = vector.MustFixedColWithTypeCheck[bool](src.Vecs[abortPos])
	}
	var commitTSs []types.TS
	if commitTSPos != -1 && !src.Vecs[commitTSPos].IsConstNull() {
		commitTSs = vector.MustFixedColWithTypeCheck[types.TS](src.Vecs[commitTSPos])
	}
	for row := 0; row < src.Length(); row++ {
		if (row < len(aborts) && aborts[row]) ||
			(row < len(commitTSs) && commitTSs[row].Equal(&txnif.UncommitTS)) {
			src.Delete(row)
		}
	}
	if src.HasDelete() {
		src.Compact()
	}
}

// marshalContainersBatch encodes bat for an api.Entry, whose attrs are
// bat.Attrs. The encoding borrows bat's buffers: the builders close their
// batches only through the callback that runs after the response is sent.
//
// A dropped column's placeholder (AppendPlaceholder) is an empty vector; it
// is encoded as a constant null of the batch's length, so every vector of
// the entry holds its row count and the column keeps its seqnum position.
func marshalContainersBatch(bat *containers.Batch) (api.EncodedBatch, error) {
	cnBat := containers.ToCNBatch(bat)
	for i, vec := range bat.Vecs {
		if vec == containers.EMPTY_VECTOR {
			cnBat.Vecs[i] = vector.NewConstNull(*vec.GetType(), bat.Length(), nil)
		}
	}
	return batch.Encode(cnBat)
}
