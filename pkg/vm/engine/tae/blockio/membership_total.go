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

// withScopedTotalMembership keeps original cached keys and predicates pinned
// through both selections. Only proven-total predicates may skip rejected rows.
// Nonborrowable inputs decline before either callback, preserving the ordinary
// predicate-first path. No temporary header or row map escapes the I/O scope.
func withScopedTotalMembership(ctx context.Context, info *objectio.BlockInfo,
	columns []uint16, typs []types.Type, early, predicates []int, rows int,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, s *engine.ReaderMembershipScratch) (handled bool, retErr error) {
	for _, column := range predicates {
		if !slices.Contains(early, column) {
			return false, moerr.NewInvalidInputNoCtx("membership predicate column outside early schema")
		}
	}
	defer func() { clear(s.Sources); clear(s.Views) }()
	read := &s.PredicateRead
	read.Clear()
	defer read.Clear()
	for _, column := range early {
		read.Seqnums = append(read.Seqnums, columns[column])
		read.Types = append(read.Types, typs[column])
		read.Sources = append(read.Sources, output.Vecs[column])
	}
	fallback := false
	rejected := 0
	_, err := ioutil.WithColumnsDataIntoBorrowedFixed(ctx, read.Seqnums, read.Types, fs, info.MetaLocation(), read.Sources, request.ColumnScope(), mp, policy, func(inputs []*vector.Vector, marked []bool) error {
		for _, cached := range marked {
			if !cached {
				// Unsupported inputs remain unloaded. Preserve the established
				// reader's clean destination contract before declining.
				for _, destination := range read.Sources {
					destination.CleanOnlyData()
				}
				return nil
			}
		}
		handled = true
		borrowed := scopedMembershipPredicates{columns: early, destinations: read.Sources}
		defer borrowed.restore(output)
		defer s.Scope.End()
		for i, column := range early {
			if inputs[i].Length() != rows {
				return moerr.NewInvalidInputNoCtx("scoped membership input row mismatch")
			}
			s.Sources[column] = inputs[i]
			output.Vecs[column] = inputs[i]
		}
		output.SetRowCount(rows)
		member, err := request.Membership.Select(output, request.Membership.Columns)
		if err != nil {
			return err
		}
		count := rows
		if !member.All {
			count = len(member.Sels)
		}
		if err := validateReaderFilterResult(member, rows, count); err != nil {
			return err
		}
		rejected = rows - count
		if count == 0 {
			borrowed.restore(output)
			output.CleanOnlyData()
			return nil
		}
		var physical []int64
		if !member.All {
			if err := s.Physical.PreExtend(count, mp); err != nil {
				if mpool.IsRetryableAllocationCapacity(err) {
					fallback = true
					return nil
				}
				return err
			}
			s.Physical.SetLength(count)
			physical = vector.MustFixedColNoTypeCheck[int64](s.Physical)
			copy(physical, member.Sels)
			admitted, err := s.Scope.Bind(s.Sources, s.Views, predicates, physical, inputs[0].Length(), mp, request.OutputAllocation)
			if err != nil && !mpool.IsRetryableAllocationCapacity(err) {
				return err
			}
			if admitted {
				for _, column := range predicates {
					output.Vecs[column] = s.Views[column]
				}
			} else {
				// Optional view metadata must not force a temporary payload
				// allocation ahead of the ordinary predicate evaluation.
				fallback = true
				return nil
			}
		}
		output.SetRowCount(count)
		residual, err := request.Select(output, predicates)
		if err != nil {
			// Only this proven-total route may retry predicates. Release its
			// optional map/views before the ordinary evaluation; without live
			// scratch a callback capacity error has no staging work to reclaim.
			if mpool.IsRetryableAllocationCapacity(err) && s.Physical.Allocated() > 0 {
				fallback = true
				return nil
			}
			return err
		}
		selected := count
		if !residual.All {
			selected = len(residual.Sels)
		}
		if err := validateReaderFilterResult(residual, count, selected); err != nil {
			return err
		}
		// Retire the views before changing the row map they borrow.
		s.Scope.End()
		if !residual.All {
			if member.All {
				// The final callback's rows already use the physical domain.
				physical = residual.Sels
			} else {
				for row, survivor := range residual.Sels {
					physical[row] = physical[survivor]
				}
			}
		}
		if physical != nil {
			physical = physical[:selected]
		}
		for i, column := range early {
			output.Vecs[column] = inputs[i]
		}
		if selected == 0 {
			borrowed.restore(output)
			output.CleanOnlyData()
			return nil
		}
		output.SetRowCount(selected)
		if err := borrowed.gather(output, physical, member.All && residual.All, request, mp); err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				fallback = true
				return nil
			}
			return err
		}
		late := columnPositionsComplement(early, len(columns))
		var readRows []int64
		if !member.All || !residual.All {
			readRows = physical
		}
		if err := loadMembershipColumns(ctx, info, columns, typs, late, readRows, policy, output, mp, fs, request); err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				fallback = true
				return nil
			}
			return err
		}
		output.SetRowCount(selected)
		return nil
	})
	if err == nil && fallback {
		// The pinned scope has returned and restored all original headers. Retire
		// every optional row-map/bitmap allocation before baseline Select runs.
		request.FreeMembership(mp)
		request.Membership.Bypass = true
		request.Membership.OutputRows += int64(rejected)
		output.CleanOnlyData()
		return true, blockDataReadScopedOwned(ctx, info, columns, typs, predicates, policy, output, mp, fs, request, nil)
	}
	return handled, err
}
