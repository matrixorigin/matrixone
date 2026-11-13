// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestScanReaderCountUsesPrunedWork(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocks  int
		width   float64
		prepare bool
		want    int
	}{
		{"few blocks", 5, 72, false, 1},
		{"many blocks", 124, 72, false, 4},
		{"wide rows", 5, 128, false, 4},
		{"ordinary grain", 32, 72, false, 3},
		{"prepared grain", 32, 72, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := &plan.Stats{BlockNum: 124, Rowsize: tc.width, HashmapStats: &plan.HashMapStats{Shuffle: true}}
			node := &plan.Node{Stats: stats}
			s := &Scope{NodeInfo: engine.Node{Mcpu: 4, Data: readutil.NewBlockListRelationData(tc.blocks)}, DataSource: &Source{node: node}}
			c := &Compile{pn: &plan.Plan{IsPrepare: tc.prepare, Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{node}}}}}
			require.Equal(t, tc.want, s.scanReaderCount(c))
			require.Equal(t, 4, s.NodeInfo.Mcpu, "reader reduction must not alter worker/EOF topology")
			require.EqualValues(t, 124, stats.BlockNum, "do not rewrite reusable plan statistics")
		})
	}
	for _, name := range []string{"no compile", "no plan", "no query", "empty query", "nil node", "missing stats", "no source stats", "ordered", "index top", "reused sibling", "object list", "combined", "empty ranges"} {
		t.Run(name, func(t *testing.T) {
			node := &plan.Node{Stats: &plan.Stats{BlockNum: 124, Rowsize: 72}}
			query := &plan.Query{Nodes: []*plan.Node{node}}
			c := &Compile{pn: &plan.Plan{Plan: &plan.Plan_Query{Query: query}}}
			s := &Scope{NodeInfo: engine.Node{Mcpu: 4, Data: readutil.NewBlockListRelationData(5)}, DataSource: &Source{node: node}}
			switch name {
			case "no compile":
				c = nil
			case "no plan":
				c.pn = nil
			case "no query":
				c.pn = &plan.Plan{}
			case "empty query":
				query.Nodes = nil
			case "nil node":
				query.Nodes = append(query.Nodes, nil)
			case "missing stats":
				query.Nodes = append(query.Nodes, &plan.Node{})
			case "no source stats":
				s.DataSource.node = &plan.Node{}
			case "ordered":
				s.DataSource.OrderBy = []*plan.OrderBySpec{{}}
			case "index top":
				s.DataSource.IndexReaderParam = &plan.IndexReaderParam{}
			case "reused sibling":
				query.Nodes = append(query.Nodes, &plan.Node{Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{ShuffleMethod: plan.ShuffleMethod_Reuse}}})
			case "object list":
				s.NodeInfo.Data = &readutil.ObjListRelData{}
			case "combined":
				s.NodeInfo.Data = &disttae.CombinedRelData{}
			case "empty ranges":
				s.NodeInfo.Data = readutil.NewBlockListRelationData(0)
			}
			require.Equal(t, 4, s.scanReaderCount(c))
		})
	}
}

type scanGroupingReader struct {
	readutil.EmptyReader
	id                int
	observed          *[]int
	delivered         bool
	closes            int
	readErr, closeErr error
}

func (r *scanGroupingReader) Read(ctx context.Context, _ []string, _ *plan.Expr, _ *mpool.MPool, out *batch.Batch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if r.readErr != nil {
		return false, r.readErr
	}
	if r.delivered {
		return true, nil
	}
	r.delivered = true
	*r.observed = append(*r.observed, r.id)
	out.SetRowCount(1)
	return false, nil
}
func (r *scanGroupingReader) ReadWithFilter(ctx context.Context, cols []string, _ []int, _ engine.ReaderFilter, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	return r.Read(ctx, cols, nil, mp, out)
}
func (r *scanGroupingReader) Close() error { r.closes++; return r.closeErr }

func TestGroupScanReadersCoverageAndCleanup(t *testing.T) {
	for _, n := range []int{0, 1, 3, 4, 5, 8, 9} {
		for _, late := range []bool{false, true} {
			t.Run(fmt.Sprintf("readers_%d/late_%t", n, late), func(t *testing.T) {
				var observed []int
				readers := make([]engine.Reader, n)
				originals := make([]*scanGroupingReader, n)
				for i := range readers {
					originals[i] = &scanGroupingReader{id: i, observed: &observed}
					readers[i] = originals[i]
				}
				groups := groupScanReaders(readers, 4)
				require.Len(t, groups, 4)
				for _, rd := range groups {
					for {
						var out batch.Batch
						var done bool
						var err error
						if lateReader, ok := rd.(engine.LateMaterializationReader); late && ok {
							done, err = lateReader.ReadWithFilter(t.Context(), nil, nil, func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
								return engine.ReaderFilterResult{All: true}, nil
							}, nil, &out)
						} else {
							done, err = rd.Read(t.Context(), nil, nil, nil, &out)
						}
						require.NoError(t, err)
						if done {
							break
						}
						require.Equal(t, 1, out.RowCount())
					}
					require.NoError(t, rd.Close())
				}
				require.Len(t, observed, n)
				for i := range observed {
					require.Equal(t, i, observed[i], "each original stream must appear exactly once in order")
				}
				for _, r := range originals {
					require.Equal(t, 1, r.closes)
				}
			})
		}
	}
}

func TestGroupScanReadersFailureCleanup(t *testing.T) {
	readErr, closeErr := errors.New("read failure"), errors.New("close failure")
	for _, mode := range []string{"read error", "cancel", "early stop"} {
		t.Run(mode, func(t *testing.T) {
			var observed []int
			originals := []*scanGroupingReader{{id: 0, observed: &observed}, {id: 1, observed: &observed, closeErr: closeErr}, {id: 2, observed: &observed}}
			if mode == "read error" {
				originals[0].readErr = readErr
			}
			readers := []engine.Reader{originals[0], originals[1], originals[2]}
			group := groupScanReaders(readers, 1)[0]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancel" {
				cancel()
			}
			var out batch.Batch
			done, err := group.Read(ctx, nil, nil, nil, &out)
			require.False(t, done)
			switch mode {
			case "read error":
				require.ErrorIs(t, err, readErr)
				require.ErrorIs(t, err, closeErr)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorIs(t, err, closeErr)
			case "early stop":
				require.NoError(t, err)
				require.ErrorIs(t, group.Close(), closeErr)
			}
			require.NoError(t, group.Close(), "repeated worker cleanup must not close transferred readers twice")
			for _, r := range originals {
				require.Equal(t, 1, r.closes)
			}
		})
	}
}
