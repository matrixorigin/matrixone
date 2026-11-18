// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// blockDataReadMembership admits only the pinned, identity-visible total path.
// Other routes keep the ordinary reader and TableScan's post-read membership.
func blockDataReadMembership(ctx context.Context, info *objectio.BlockInfo, ds engine.DataSource,
	columns []uint16, typs []types.Type, storageRows []int64, early []int,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, preFilterRows *int) (handled bool, retErr error) {
	if request == nil || request.Membership == nil {
		return false, nil
	}
	defer func() {
		if !handled {
			// A previous admitted block may have retained optional capacity.
			// Restore that headroom before the ordinary reader allocates.
			request.FreeMembership(mp)
		}
	}()
	member := request.Membership
	if member.Bypass || !member.PredicatesTotal || request.Exact != nil || storageRows != nil ||
		member.Select == nil || request.Select == nil || ds == nil || request.OutputAllocation == nil ||
		len(member.Columns) == 0 || len(output.ExtraBuf) != 0 || info.IsAppendable() ||
		info.MetaLocation().Rows() > objectio.BlockMaxRows || objectio.JSONReadSetFromContext(ctx).MatchesJSONColumns(typs) {
		return false, nil
	}
	for i, column := range member.Columns {
		if column < 0 || column >= len(columns) || !slices.Contains(early, column) ||
			(i > 0 && member.Columns[i-1] >= column) {
			return false, nil
		}
	}
	if len(member.ColumnUses) == len(columns) {
		original := request.ColumnUses
		request.ColumnUses = member.ColumnUses
		defer func() { request.ColumnUses = original }()
	}
	if !scopedOwnedColumns(request, len(columns)) {
		return false, nil
	}
	predicates := member.PredicateColumns
	if predicates == nil {
		predicates = early
	}
	if len(predicates) == 0 {
		return false, nil
	}
	deleted, err := ds.GetTombstones(ctx, &info.BlockID)
	identity := deleted.IsEmpty()
	deleted.Release()
	if err != nil {
		return true, err
	}
	if !identity {
		return false, nil
	}
	rows := int(info.MetaLocation().Rows())
	if preFilterRows != nil {
		*preFilterRows = rows
	}
	if rows == 0 {
		output.SetRowCount(0)
		return true, nil
	}
	s, err := request.MembershipScratch(len(columns))
	if err != nil {
		return false, err
	}
	return withScopedTotalMembership(ctx, info, columns, typs, early, predicates, rows, policy, output, mp, fs, request, s)
}

func loadMembershipColumns(ctx context.Context, info *objectio.BlockInfo, columns []uint16, typs []types.Type,
	positions []int, rows []int64, policy fileservice.Policy, output *batch.Batch,
	mp *mpool.MPool, fs fileservice.FileService, request *engine.ReaderFilterRequest) error {
	if len(positions) == 0 {
		return nil
	}
	if rows != nil {
		ignored, err := materializeBlockColumnsAtPositions(ctx, info, columns, typs, -1, positions, rows,
			nil, policy, output, mp, fs)
		ignored.Release()
		return err
	}
	scratch := request.SelectionScratch(len(positions))
	defer scratch.Clear()
	for _, position := range positions {
		scratch.Seqnums = append(scratch.Seqnums, columns[position])
		scratch.Types = append(scratch.Types, typs[position])
		scratch.Sources = append(scratch.Sources, output.Vecs[position])
	}
	_, err := ioutil.LoadColumnsDataIntoBorrowed(ctx, scratch.Seqnums, scratch.Types, fs, info.MetaLocation(), scratch.Sources,
		mp, policy, request.BorrowColumns(positions, len(output.Vecs)))
	if err != nil {
		return err
	}
	rowsInColumn := output.Vecs[positions[0]].Length()
	for _, position := range positions[1:] {
		if output.Vecs[position].Length() != rowsInColumn {
			return moerr.NewInvalidInputNoCtx("membership read left inconsistent column lengths")
		}
	}
	return nil
}

// scopedMembershipPredicates borrows no ownership. Its cached inputs remain
// protected by WithColumnsDataInto until final selected gathering completes.
type scopedMembershipPredicates struct {
	columns      []int
	destinations []*vector.Vector
}

func (p *scopedMembershipPredicates) restore(output *batch.Batch) {
	for i, column := range p.columns {
		output.Vecs[column] = p.destinations[i]
	}
}
func (p *scopedMembershipPredicates) gather(output *batch.Batch, rows []int64, all bool, request *engine.ReaderFilterRequest, mp *mpool.MPool) error {
	for i, column := range p.columns {
		source, destination := output.Vecs[column], p.destinations[i]
		if request.ColumnUse(column, len(output.Vecs)) == engine.ReaderColumnUnused {
			destination.CleanOnlyData()
			output.Vecs[column] = destination
			continue
		}
		var err error
		if all {
			err = destination.UnionBatch(source, 0, output.RowCount(), nil, mp)
		} else {
			err = destination.Union(source, rows, mp)
		}
		if err != nil {
			return err
		}
		source.CopyExpressionMetadataTo(destination)
		output.Vecs[column] = destination
	}
	if request.ObserveSelection != nil {
		request.ObserveSelection(output.RowCount(), 0, 0)
	}
	return nil
}
