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
	"errors"
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

// blockDataReadMembership is optional before any predicate callback or I/O.
// Storage selections already include visibility. Without them, a persisted
// block's tombstones establish the original physical domain before membership.
func blockDataReadMembership(ctx context.Context, info *objectio.BlockInfo, ds engine.DataSource,
	columns []uint16, typs []types.Type, storageRows []int64, early []int,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, preFilterRows *int) (handled bool, retErr error) {
	if request == nil || request.Membership == nil || request.Membership.Select == nil || request.Select == nil ||
		request.OutputAllocation == nil || len(request.Membership.Columns) == 0 || len(output.ExtraBuf) != 0 ||
		info.IsAppendable() || info.MetaLocation().Rows() > objectio.BlockMaxRows || objectio.JSONReadSetFromContext(ctx).MatchesJSONColumns(typs) {
		return false, nil
	}
	for i, column := range request.Membership.Columns {
		if column < 0 || column >= len(columns) || !slices.Contains(early, column) ||
			(i > 0 && request.Membership.Columns[i-1] >= column) {
			return false, nil
		}
	}
	s, err := request.MembershipScratch(len(columns))
	if err != nil {
		return false, err
	}
	for _, v := range []*vector.Vector{s.Physical, s.Logical} {
		if err = v.PreExtend(int(info.MetaLocation().Rows()), mp); err != nil {
			if errors.Is(err, mpool.ErrAllocationAccountCapacity) {
				request.FreeMembership(mp)
				return false, nil
			}
			return false, err
		}
		v.SetLength(int(info.MetaLocation().Rows()))
	}
	physical := vector.MustFixedColNoTypeCheck[int64](s.Physical)
	logical := vector.MustFixedColNoTypeCheck[int64](s.Logical)
	live := 0
	identity := storageRows == nil
	if storageRows != nil {
		live = copy(physical, storageRows)
	} else {
		if ds == nil {
			return false, nil
		}
		deleted, err := ds.GetTombstones(ctx, &info.BlockID)
		if err != nil {
			deleted.Release()
			return true, err
		}
		identity = deleted.IsEmpty()
		for row := range int(info.MetaLocation().Rows()) {
			if !deleted.Contains(uint64(row)) {
				physical[live] = int64(row)
				live++
			}
		}
		deleted.Release()
	}
	if preFilterRows != nil {
		*preFilterRows = live
	}
	if live == 0 {
		output.SetRowCount(0)
		return true, nil
	}
	var keyReadRows []int64
	if !identity {
		keyReadRows = physical[:live]
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, request.Membership.Columns, keyReadRows,
		policy, output, mp, fs, request); err != nil {
		return true, err
	}
	output.SetRowCount(live)
	member, err := request.Membership.Select(output, request.Membership.Columns)
	if err != nil {
		return true, err
	}
	count := live
	if !member.All {
		count = len(member.Sels)
	}
	if err := validateReaderFilterResult(member, live, count); err != nil {
		return true, err
	}
	if count == 0 {
		output.CleanOnlyData()
		return true, nil
	}
	for row := range count {
		original := int64(row)
		if !member.All {
			original = member.Sels[row]
		}
		logical[row] = original
		physical[row] = physical[original]
	}
	physical, logical = physical[:count], logical[:count]
	if request.Exact != nil {
		// Preserve the existing exact program and its owned-payload construction;
		// only its original physical input rows are narrowed by membership.
		output.CleanOnlyData()
		return true, blockDataReadExactFilter(ctx, info, ds, columns, typs, physical, policy, output, mp, fs, request, nil)
	}
	keyDomain := live
	bound := false
	restore := func() {
		if bound {
			for _, column := range request.Membership.Columns {
				output.Vecs[column] = s.Sources[column]
			}
			s.Scope.End()
			bound = false
		}
	}
	defer func() {
		restore()
		clear(s.Sources)
		clear(s.Views)
		s.Predicates = s.Predicates[:0]
	}()
	if !member.All {
		for _, column := range request.Membership.Columns {
			s.Sources[column] = output.Vecs[column]
		}
		admitted, scopeErr := s.Scope.Bind(s.Sources, s.Views, request.Membership.Columns, logical, live, mp, request.OutputAllocation)
		if scopeErr != nil && !errors.Is(scopeErr, mpool.ErrAllocationAccountCapacity) {
			return true, scopeErr
		}
		if admitted {
			bound = true
			for _, column := range request.Membership.Columns {
				output.Vecs[column] = s.Views[column]
			}
		} else {
			// Scope admission is optional. Compact only the already loaded key
			// on refusal; do not read rejected rows' predicate or payload values.
			s.Scope.End()
			for _, column := range request.Membership.Columns {
				if err := output.Vecs[column].Shrink(logical, false, mp); err != nil {
					return true, err
				}
			}
			keyDomain = count
			for row := range logical {
				logical[row] = int64(row)
			}
		}
	}
	s.Predicates = s.Predicates[:0]
	for _, column := range early {
		if !slices.Contains(request.Membership.Columns, column) {
			s.Predicates = append(s.Predicates, column)
		}
	}
	var predicateReadRows []int64
	if !identity || !member.All {
		predicateReadRows = physical
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, s.Predicates, predicateReadRows,
		policy, output, mp, fs, request); err != nil {
		return true, err
	}
	output.SetRowCount(count)
	residual, err := request.Select(output, early)
	if err != nil {
		return true, err
	}
	selected := count
	if !residual.All {
		selected = len(residual.Sels)
	}
	if err := validateReaderFilterResult(residual, count, selected); err != nil {
		return true, err
	}
	restore()
	if selected == 0 {
		output.CleanOnlyData()
		return true, nil
	}
	if !residual.All {
		for row, survivor := range residual.Sels {
			logical[row] = logical[survivor]
			physical[row] = physical[survivor]
		}
		logical, physical = logical[:selected], physical[:selected]
	}
	if !member.All || !residual.All {
		output.SetRowCount(keyDomain)
		if err := publishReaderSelection(output, request.Membership.Columns, logical, request, mp); err != nil {
			return true, err
		}
	}
	if !residual.All && len(s.Predicates) > 0 {
		output.SetRowCount(count)
		if err := publishReaderSelection(output, s.Predicates, residual.Sels, request, mp); err != nil {
			return true, err
		}
	}
	late := columnPositionsComplement(early, len(columns))
	var payloadRows []int64
	if !identity || !member.All || !residual.All {
		payloadRows = physical
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, late, payloadRows, policy, output, mp, fs, request); err != nil {
		return true, err
	}
	output.SetRowCount(selected)
	return true, nil
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
