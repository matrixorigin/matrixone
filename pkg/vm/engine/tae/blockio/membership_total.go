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

// withScopedTotalMembership keeps original cached keys and predicates pinned
// through both selections. Only proven-total predicates may skip rejected rows.
// Nonborrowable inputs decline before either callback, preserving the ordinary
// predicate-first path. No temporary header or row map escapes the I/O scope.
func withScopedTotalMembership(ctx context.Context, info *objectio.BlockInfo,
	columns []uint16, typs []types.Type, early, predicates []int, physical []int64,
	policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	request *engine.ReaderFilterRequest, s *engine.ReaderMembershipScratch) (handled bool, retErr error) {
	read := &s.PredicateRead
	read.Clear()
	defer read.Clear()
	for _, column := range early {
		read.Seqnums = append(read.Seqnums, columns[column])
		read.Types = append(read.Types, typs[column])
		read.Sources = append(read.Sources, output.Vecs[column])
	}
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
		borrowed := scopedMembershipPredicates{columns: early, destinations: read.Sources, marked: marked}
		defer borrowed.restore(output)
		defer s.Scope.End()
		for i, column := range early {
			if inputs[i].Length() != len(physical) {
				return moerr.NewInvalidInputNoCtx("scoped membership input row mismatch")
			}
			s.Sources[column] = inputs[i]
			output.Vecs[column] = inputs[i]
		}
		output.SetRowCount(len(physical))
		member, err := request.Membership.Select(output, request.Membership.Columns)
		if err != nil {
			return err
		}
		count := len(physical)
		if !member.All {
			count = len(member.Sels)
		}
		if err := validateReaderFilterResult(member, len(physical), count); err != nil {
			return err
		}
		if count == 0 {
			borrowed.restore(output)
			output.CleanOnlyData()
			return nil
		}
		if !member.All {
			copy(physical, member.Sels)
		}
		physical = physical[:count]
		copiedPredicates := false
		if !member.All {
			admitted, err := s.Scope.Bind(s.Sources, s.Views, predicates, physical, inputs[0].Length(), mp, request.OutputAllocation)
			if err != nil && !errors.Is(err, mpool.ErrAllocationAccountCapacity) {
				return err
			}
			if admitted {
				for _, column := range predicates {
					output.Vecs[column] = s.Views[column]
				}
			} else {
				s.Scope.Free(mp)
				copiedPredicates = true
				// Reuse the ordinary owned gather when optional view metadata
				// is refused. Original cached inputs remain live for publication.
				for i, column := range early {
					if !slices.Contains(predicates, column) {
						continue
					}
					destination := read.Sources[i]
					if err := destination.Union(inputs[i], physical, mp); err != nil {
						return err
					}
					inputs[i].CopyExpressionMetadataTo(destination)
					output.Vecs[column] = destination
				}
			}
		}
		output.SetRowCount(count)
		residual, err := request.Select(output, predicates)
		if err != nil {
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
				// Membership did not write the lazy identity map.
				copy(physical, residual.Sels)
			} else {
				for row, survivor := range residual.Sels {
					physical[row] = physical[survivor]
				}
			}
		}
		physical = physical[:selected]
		for i, column := range early {
			if copiedPredicates && slices.Contains(predicates, column) {
				read.Sources[i].CleanOnlyData()
			}
			output.Vecs[column] = inputs[i]
		}
		if selected == 0 {
			borrowed.restore(output)
			output.CleanOnlyData()
			return nil
		}
		if err := borrowed.gather(output, physical, member.All && residual.All, request, mp); err != nil {
			return err
		}
		late := columnPositionsComplement(early, len(columns))
		var readRows []int64
		if !member.All || !residual.All {
			readRows = physical
		}
		if err := loadMembershipColumns(ctx, info, columns, typs, late, readRows, policy, output, mp, fs, request); err != nil {
			return err
		}
		output.SetRowCount(selected)
		return nil
	})
	return handled, err
}
