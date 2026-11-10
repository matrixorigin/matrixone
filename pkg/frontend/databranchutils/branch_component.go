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

package databranchutils

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type BranchComponentQuery func(context.Context, string) (executor.Result, error)

// LoadLockedBranchComponents reads only the components containing tableIDs.
// query must use the same transaction, as sys, without a statement boundary.
// Before any mutation, the caller must hold the shared lifecycle gate that
// excludes connector deletion and catalog replacement. admit takes exact X
// locks on the sorted root table_id keys (including absent rows), installs an
// applied RC frontier, and verifies the metadata relation's physical identity.
// It must not release those locks before the owning transaction ends.
func LoadLockedBranchComponents(
	ctx context.Context, tableIDs []uint64, query BranchComponentQuery,
	admit func([]uint64) error,
) (BranchReclaimDag, error) {
	loader := branchComponentLoader{ctx: ctx, query: query}
	roots, _, err := loader.ancestors(tableIDs)
	if err != nil || len(roots) == 0 {
		return BranchReclaimDag{}, err
	}
	if err = admit(roots); err != nil {
		return BranchReclaimDag{}, err
	}
	current, ancestors, err := loader.ancestors(tableIDs)
	if err != nil {
		return BranchReclaimDag{}, err
	}
	if !slices.Equal(roots, current) {
		return BranchReclaimDag{}, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	rows := make(map[uint64]DataBranchMetadata)
	for _, root := range roots {
		if row, ok := ancestors[root]; ok {
			rows[root] = row
		}
	}
	frontier := roots
	for len(frontier) > 0 {
		children, err := loader.read("p_table_id", frontier)
		if err != nil {
			return BranchReclaimDag{}, err
		}
		frontier = nil
		for id, row := range children {
			if old, ok := rows[id]; ok {
				if old != row {
					return BranchReclaimDag{}, moerr.NewInternalError(ctx, "contradictory branch component identity")
				}
				continue
			}
			rows[id] = row
			frontier = append(frontier, id)
		}
	}
	return NewBranchReclaimDag(slices.Collect(maps.Values(rows))), nil
}

type branchComponentLoader struct {
	ctx   context.Context
	query BranchComponentQuery
}

func (l branchComponentLoader) ancestors(ids []uint64) ([]uint64, map[uint64]DataBranchMetadata, error) {
	rows := make(map[uint64]DataBranchMetadata)
	seen := make(map[uint64]bool)
	frontier := slices.Clone(ids)
	for len(frontier) > 0 {
		pending := frontier[:0]
		for _, id := range frontier {
			if id == 0 {
				return nil, nil, moerr.NewInternalError(l.ctx, "zero branch component identity")
			}
			if !seen[id] {
				seen[id] = true
				pending = append(pending, id)
			}
		}
		found, err := l.read("table_id", pending)
		if err != nil {
			return nil, nil, err
		}
		frontier = nil
		for id, row := range found {
			rows[id] = row
			if row.PTableID != 0 && !seen[row.PTableID] {
				frontier = append(frontier, row.PTableID)
			}
		}
	}
	resolved := make(map[uint64]uint64)
	roots := make(map[uint64]struct{})
	for _, id := range ids {
		path := make(map[uint64]struct{})
		root := id
		for {
			if err := l.ctx.Err(); err != nil {
				return nil, nil, err
			}
			if cached, ok := resolved[root]; ok {
				root = cached
				break
			}
			if _, cycle := path[root]; cycle {
				return nil, nil, moerr.NewInternalError(l.ctx, "cyclic branch component")
			}
			path[root] = struct{}{}
			row, ok := rows[root]
			if !ok || row.PTableID == 0 {
				break
			}
			root = row.PTableID
		}
		for id := range path {
			resolved[id] = root
		}
		roots[root] = struct{}{}
	}
	return slices.Sorted(maps.Keys(roots)), rows, nil
}

func (l branchComponentLoader) read(column string, ids []uint64) (map[uint64]DataBranchMetadata, error) {
	rows := make(map[uint64]DataBranchMetadata)
	for len(ids) > 0 {
		if err := l.ctx.Err(); err != nil {
			return nil, err
		}
		n := min(len(ids), 128)
		keys := make([]string, n)
		for i, id := range ids[:n] {
			keys[i] = strconv.FormatUint(id, 10)
		}
		sql := fmt.Sprintf("select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata where %s in (%s)", column, strings.Join(keys, ","))
		err := func() error {
			res, err := l.query(l.ctx, sql)
			defer res.Close()
			if err != nil {
				return err
			}
			res.ReadRows(func(n int, cols []*vector.Vector) bool {
				kinds := [...]types.T{types.T_uint64, types.T_uint64, types.T_int64, types.T_uint64, types.T_varchar, types.T_bool}
				if len(cols) != len(kinds) {
					err = moerr.NewInternalError(l.ctx, "invalid branch component columns")
					return false
				}
				for i, kind := range kinds {
					if cols[i] == nil || cols[i].GetType().Oid != kind || cols[i].Length() != n || cols[i].GetNulls().Any() {
						err = moerr.NewInternalError(l.ctx, "invalid branch component column")
						return false
					}
				}
				for i := 0; i < n; i++ {
					if i%128 == 0 {
						if err = l.ctx.Err(); err != nil {
							return false
						}
					}
					row := DataBranchMetadata{
						TableID:      vector.GetFixedAtWithTypeCheck[uint64](cols[0], i),
						PTableID:     vector.GetFixedAtWithTypeCheck[uint64](cols[1], i),
						CloneTS:      vector.GetFixedAtWithTypeCheck[int64](cols[2], i),
						Creator:      vector.GetFixedAtWithTypeCheck[uint64](cols[3], i),
						Level:        cols[4].GetStringAt(i),
						TableDeleted: vector.GetFixedAtWithTypeCheck[bool](cols[5], i),
					}
					if old, ok := rows[row.TableID]; row.TableID == 0 || (ok && old != row) {
						err = moerr.NewInternalError(l.ctx, "contradictory branch component identity")
						return false
					}
					rows[row.TableID] = row
				}
				return true
			})
			return err
		}()
		if err != nil {
			return nil, err
		}
		ids = ids[n:]
	}
	return rows, nil
}
