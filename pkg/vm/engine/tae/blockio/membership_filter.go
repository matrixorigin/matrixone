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
	if request == nil || request.Membership == nil || request.Membership.Bypass || request.Membership.Select == nil || request.Select == nil ||
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
		if identity {
			// Identity coordinates are derived at the first real selection.
			// Do not fill a map that the membership-first path overwrites.
			live = int(info.MetaLocation().Rows())
		} else {
			for row := range int(info.MetaLocation().Rows()) {
				if !deleted.Contains(uint64(row)) {
					physical[live] = int64(row)
					live++
				}
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
	// Only an admitted reader changes private-input publication. Restore the
	// conservative outer policy before any ordinary read fallback can run.
	if len(request.Membership.ColumnUses) == len(columns) {
		original := request.ColumnUses
		request.ColumnUses = request.Membership.ColumnUses
		defer func() { request.ColumnUses = original }()
	}
	if request.Exact == nil {
		return true, blockDataReadPredicateMembership(ctx, info, columns, typs, early, physical[:live], logical[:live], identity, policy, output, mp, fs, request, s)
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
		if identity {
			physical[row] = original
		} else {
			physical[row] = physical[original]
		}
	}
	physical = physical[:count]
	// Preserve the existing native exact program and its owned-payload path.
	output.CleanOnlyData()
	return true, blockDataReadExactFilter(ctx, info, ds, columns, typs, physical, policy, output, mp, fs, request, nil)
}

// blockDataReadPredicateMembership keeps generic predicate work on the original
// live domain, then tests membership only for its survivors. Payload remains
// unloaded until both maps have been composed back to physical block rows.
func blockDataReadPredicateMembership(ctx context.Context, info *objectio.BlockInfo,
	columns []uint16, typs []types.Type, early []int, physical, logical []int64, identity bool,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, s *engine.ReaderMembershipScratch) error {
	predicates := request.Membership.PredicateColumns
	if predicates == nil {
		predicates = early
	}
	for _, column := range predicates {
		if !slices.Contains(early, column) {
			return moerr.NewInvalidInputNoCtx("membership predicate column outside early schema")
		}
	}
	s.Predicates = append(s.Predicates[:0], predicates...)
	s.Keys, s.Overlap = s.Keys[:0], s.Overlap[:0]
	for _, column := range request.Membership.Columns {
		if slices.Contains(predicates, column) {
			s.Overlap = append(s.Overlap, column)
		} else {
			s.Keys = append(s.Keys, column)
		}
	}
	defer func() {
		clear(s.Sources)
		clear(s.Views)
		s.Predicates = s.Predicates[:0]
		s.Keys = s.Keys[:0]
		s.Overlap = s.Overlap[:0]
	}()
	if identity && request.Membership.PredicatesTotal && len(predicates) > 0 && scopedOwnedColumns(request, len(columns)) {
		handled, err := withScopedTotalMembership(ctx, info, columns, typs, early, predicates, physical, policy, output, mp, fs, request, s)
		if handled || err != nil {
			return err
		}
	}
	var readRows []int64
	if !identity {
		readRows = physical
	}
	if identity && len(predicates) > 0 && len(s.Overlap) == 0 && scopedOwnedColumns(request, len(columns)) {
		return withScopedMembershipPredicates(ctx, info, columns, typs, early, predicates, physical, logical, policy, output, mp, fs, request, s)
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, predicates, readRows, policy, output, mp, fs, request); err != nil {
		return err
	}
	return finishPredicateMembership(ctx, info, columns, typs, early, predicates, physical, logical, identity, policy, output, mp, fs, request, s, nil)
}

func finishPredicateMembership(ctx context.Context, info *objectio.BlockInfo,
	columns []uint16, typs []types.Type, early, predicates []int, physical, logical []int64, identity bool,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, s *engine.ReaderMembershipScratch, scoped *scopedMembershipPredicates) error {
	live := len(physical)
	bound := false
	restore := func() {
		if bound {
			for _, column := range s.Overlap {
				output.Vecs[column] = s.Sources[column]
			}
			s.Scope.End()
			bound = false
		}
	}
	defer restore()
	output.SetRowCount(live)
	residual, err := request.Select(output, predicates)
	if err != nil {
		return err
	}
	count := live
	if !residual.All {
		count = len(residual.Sels)
	}
	if err := validateReaderFilterResult(residual, live, count); err != nil {
		return err
	}
	if count == 0 {
		if scoped != nil {
			scoped.restore(output)
		}
		output.CleanOnlyData()
		return nil
	}
	// Consume the borrowed residual map before calling Membership. Sorted,
	// unique logical indexes make this in-place prefix composition safe.
	for row := range count {
		original := int64(row)
		if !residual.All {
			original = residual.Sels[row]
		}
		logical[row] = original
		if identity {
			physical[row] = original
		} else {
			physical[row] = physical[original]
		}
	}
	physical, logical = physical[:count], logical[:count]
	var readRows []int64
	if !identity || !residual.All {
		readRows = physical
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, s.Keys, readRows, policy, output, mp, fs, request); err != nil {
		return err
	}
	if !residual.All && len(s.Overlap) > 0 {
		for _, column := range s.Overlap {
			s.Sources[column] = output.Vecs[column]
		}
		admitted, err := s.Scope.Bind(s.Sources, s.Views, s.Overlap, logical, live, mp, request.OutputAllocation)
		if err != nil && !errors.Is(err, mpool.ErrAllocationAccountCapacity) {
			return err
		}
		if admitted {
			bound = true
			for _, column := range s.Overlap {
				output.Vecs[column] = s.Views[column]
			}
		} else {
			s.Scope.Free(mp)
			// Ordinary accounted compaction on refusal. These keys now use
			// the post-predicate domain, like membership-only key columns.
			for _, column := range s.Overlap {
				if err := output.Vecs[column].Shrink(logical, false, mp); err != nil {
					return err
				}
				s.Keys = append(s.Keys, column)
			}
			s.Predicates = s.Predicates[:0]
			for _, column := range predicates {
				if !slices.Contains(s.Overlap, column) {
					s.Predicates = append(s.Predicates, column)
				}
			}
		}
	}
	output.SetRowCount(count)
	member, err := request.Membership.Select(output, request.Membership.Columns)
	if err != nil {
		return err
	}
	selected := count
	if !member.All {
		selected = len(member.Sels)
	}
	if err := validateReaderFilterResult(member, count, selected); err != nil {
		return err
	}
	restore()
	if selected == 0 {
		if scoped != nil {
			scoped.restore(output)
		}
		output.CleanOnlyData()
		return nil
	}
	if !member.All {
		for row, survivor := range member.Sels {
			logical[row] = logical[survivor]
			physical[row] = physical[survivor]
		}
		logical, physical = logical[:selected], physical[:selected]
	}
	if scoped != nil {
		output.SetRowCount(selected)
		if err := scoped.gather(output, logical, residual.All && member.All, request, mp); err != nil {
			return err
		}
	} else if (!residual.All || !member.All) && len(s.Predicates) > 0 {
		output.SetRowCount(live)
		if err := publishReaderSelection(output, s.Predicates, logical, request, mp); err != nil {
			return err
		}
	}
	if !member.All && len(s.Keys) > 0 {
		// Publisher classification consumes sorted schema positions.
		slices.Sort(s.Keys)
		output.SetRowCount(count)
		if err := publishReaderSelection(output, s.Keys, member.Sels, request, mp); err != nil {
			return err
		}
	}
	late := columnPositionsComplement(early, len(columns))
	readRows = nil
	if !identity || !residual.All || !member.All {
		readRows = physical
	}
	if err := loadMembershipColumns(ctx, info, columns, typs, late, readRows, policy, output, mp, fs, request); err != nil {
		return err
	}
	output.SetRowCount(selected)
	return nil
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
	marked       []bool
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
		if p.marked[i] {
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
		} else if !all {
			if err := destination.Shrink(rows, false, mp); err != nil {
				return err
			}
		}
		output.Vecs[column] = destination
	}
	if request.ObserveSelection != nil {
		request.ObserveSelection(output.RowCount(), 0, 0)
	}
	return nil
}
func withScopedMembershipPredicates(ctx context.Context, info *objectio.BlockInfo, columns []uint16, typs []types.Type,
	early, predicates []int, physical, logical []int64, policy fileservice.Policy, output *batch.Batch,
	mp *mpool.MPool, fs fileservice.FileService, request *engine.ReaderFilterRequest, s *engine.ReaderMembershipScratch) error {
	read := &s.PredicateRead
	read.Clear()
	defer read.Clear()
	for _, column := range predicates {
		read.Seqnums = append(read.Seqnums, columns[column])
		read.Types = append(read.Types, typs[column])
		read.Sources = append(read.Sources, output.Vecs[column])
	}
	_, err := ioutil.WithColumnsDataInto(ctx, read.Seqnums, read.Types, fs, info.MetaLocation(), read.Sources, request.ColumnScope(), mp, policy, func(inputs []*vector.Vector, marked []bool) error {
		borrowed := scopedMembershipPredicates{columns: predicates, destinations: read.Sources, marked: marked}
		for i, column := range predicates {
			output.Vecs[column] = inputs[i]
		}
		defer borrowed.restore(output)
		for _, input := range inputs {
			if input.Length() != len(physical) {
				return moerr.NewInvalidInputNoCtx("scoped membership predicate row mismatch")
			}
		}
		return finishPredicateMembership(ctx, info, columns, typs, early, predicates, physical, logical, true, policy, output, mp, fs, request, s, &borrowed)
	})
	return err
}
