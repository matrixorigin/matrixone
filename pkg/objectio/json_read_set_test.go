// Copyright 2026 Matrix Origin
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

package objectio

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestJSONReadSetMetadataAdmissionAndRelease(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	var live uint64
	admission := func(capacity uint64) (func(), error) {
		live += capacity
		return func() { live -= capacity }, nil
	}
	empty, err := NewJSONReadSetWithAdmission(nil, nil, admission)
	require.NoError(t, err)
	require.Nil(t, empty)
	require.Zero(t, live)
	set, err := NewJSONReadSetWithAdmission([]int{0, 1}, nil, admission)
	require.NoError(t, err)
	require.NotNil(t, set)
	defer set.Close()
	initial := live
	require.Positive(t, initial)
	columns := []*vector.Vector{vector.NewVec(types.T_json.ToType()), vector.NewVec(types.T_json.ToType())}
	defer func() {
		for _, column := range columns {
			column.Free(mp)
		}
	}()
	for _, column := range columns {
		doc, e := types.ParseStringToByteJson(`{"keep":1}`)
		require.NoError(t, e)
		require.NoError(t, vector.AppendByteJson(column, doc, false, mp))
	}
	require.NoError(t, set.PublishMutableColumns(columns, mp))
	require.Greater(t, live, initial, "published reader metadata is separately charged")
	set.Close()
	set.Close()
	require.Zero(t, live)
	require.Nil(t, set.readers)
	require.Nil(t, set.sequences)
	require.Nil(t, set.requested)

	want := errors.New("scope admission rejected")
	partial := 0
	set, err = NewJSONReadSetWithAdmission([]int{0}, nil, func(uint64) (func(), error) {
		return func() { partial++ }, want
	})
	require.ErrorIs(t, err, want)
	require.Nil(t, set)
	require.Equal(t, 1, partial)
	_, err = NewJSONReadSetWithAdmission([]int{0}, nil, nil)
	require.Error(t, err)
}

func TestJSONReadSetOwnsPublishedVersions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := vector.NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	value, err := types.ParseStringToByteJson("1")
	require.NoError(t, err)
	raw, err := value.Marshal()
	require.NoError(t, err)
	require.NoError(t, builder.Append(raw, false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	set := NewJSONReadSet([]int{1})
	defer set.Close()
	ctx := WithJSONReadSet(context.Background(), set)
	require.Same(t, set, JSONReadSetFromContext(ctx))
	require.False(t, set.Requested(9))
	set.BindColumns([]uint16{8, 9})
	require.False(t, set.Requested(8))
	require.True(t, set.Requested(9))
	first, err := reader.Window(0, 0)
	require.NoError(t, err)
	set.Publish(9, first)
	require.Same(t, first, set.Readers()[1])
	second, err := reader.Window(0, 0)
	require.NoError(t, err)
	set.Publish(9, second)
	require.Nil(t, set.Readers(), "ambiguous repeated publication must disable reuse")
	_, err = first.Window(0, 0)
	require.Error(t, err)
	_, err = second.Window(0, 0)
	require.Error(t, err)
	set.Close()
	// Close the extra probe reference as well as the original reader.
	probe, err := reader.Window(0, 0)
	require.NoError(t, err)
	probe.Close()
}

func TestJSONReadSetDeleteMapping(t *testing.T) {
	for _, deleted := range [][]int64{{1}, {0, 1, 2}, {2, 1}, {3}, {-1}} {
		t.Run(fmt.Sprint(deleted), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			builder, err := vector.NewJSONReaderBuilder(mp, nil)
			require.NoError(t, err)
			defer builder.Close()
			for _, text := range []string{"10", "20", "30"} {
				value, err := types.ParseStringToByteJson(text)
				require.NoError(t, err)
				raw, err := value.Marshal()
				require.NoError(t, err)
				require.NoError(t, builder.Append(raw, false))
			}
			reader, err := builder.Finish()
			require.NoError(t, err)
			set := NewJSONReadSet([]int{0})
			defer set.Close()
			set.BindColumns([]uint16{7})
			set.Publish(7, reader)
			err = set.ApplyDeletes(deleted, mp)
			if deleted[0] == 1 {
				require.NoError(t, err)
				mapped := set.Readers()[0]
				require.Equal(t, 2, mapped.Length())
				path, err := types.ParseStringToPath("$")
				require.NoError(t, err)
				for row, want := range []string{"10", "30"} {
					result, err := mapped.Query(row, &path)
					require.NoError(t, err)
					text, err := result.Text()
					result.Close()
					require.NoError(t, err)
					require.Equal(t, want, text.Text)
					text.Close()
				}
			} else if len(deleted) == 3 {
				require.NoError(t, err)
				require.Zero(t, set.Readers()[0].Length())
			} else {
				require.Error(t, err)
				require.Nil(t, set.Readers())
			}
		})
	}
}

type publicationAllocationController struct {
	calls, failAt int
	used          uint64
}

func (c *publicationAllocationController) AcquireAllocationCapacity(size uint64) error {
	c.calls++
	if c.calls == c.failAt {
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	return nil
}
func (c *publicationAllocationController) ReleaseAllocationCapacity(size uint64) { c.used -= size }

func TestJSONReadSetPublicationAllocationFailure(t *testing.T) {
	for _, operation := range []string{"partial-publication", "mapping"} {
		t.Run(operation, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			defer func() {
				require.Zero(t, account.Seal().Used)
				require.Zero(t, registry.LiveAllocationMetadata())
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			controller := new(publicationAllocationController)
			class, err := account.RegisterCapacityController(controller)
			require.NoError(t, err)
			defer func() {
				require.Zero(t, controller.used)
				require.NoError(t, account.UnregisterCapacityController(class, controller))
			}()
			selection, err := vector.NewAllocationAccountSelectionWithCapacityClass(account, 1, 1, 2, 3, 4, class)
			require.NoError(t, err)
			set := NewJSONReadSet([]int{0, 1}, selection)
			defer set.Close()
			if operation == "partial-publication" {
				source := vector.NewVec(types.T_json.ToType())
				defer source.Free(mp)
				value, err := types.ParseStringToByteJson("1")
				require.NoError(t, err)
				require.NoError(t, vector.AppendByteJson(source, value, false, mp))
				controller.failAt = 2
				require.Error(t, set.PublishMutableColumns([]*vector.Vector{source, source}, mp))
				require.Equal(t, 2, controller.calls, "first column must have been published before failure")
			} else {
				set = NewJSONReadSet([]int{0}, selection)
				defer set.Close()
				builder, err := vector.NewJSONReaderBuilder(mp, selection)
				require.NoError(t, err)
				defer builder.Close()
				require.NoError(t, builder.Append(nil, true))
				require.NoError(t, builder.Append(nil, true))
				reader, err := builder.Finish()
				require.NoError(t, err)
				set.BindColumns([]uint16{0})
				set.Publish(0, reader)
				controller.calls = 0
				controller.failAt = 1
				require.Error(t, set.ApplyDeletes([]int64{1}, mp))
			}
			require.Nil(t, set.Readers())
			require.Zero(t, controller.used)
		})
	}
}

func TestJSONReadSetCanRebindAfterEmptyChild(t *testing.T) {
	set := NewJSONReadSet([]int{0})
	defer set.Close()
	set.BindColumns([]uint16{8})
	require.True(t, set.Requested(8))
	set.BindColumns([]uint16{9})
	require.False(t, set.Requested(8))
	require.True(t, set.Requested(9), "empty exhausted child transfers no qualification")
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := vector.NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(nil, true))
	reader, err := builder.Finish()
	require.NoError(t, err)
	set.Publish(9, reader)
	set.BindColumns([]uint16{10})
	require.Nil(t, set.Readers(), "rebind with a published version must discard ambiguous qualification")
}

func TestJSONReadSetDeclinesMalformedInputForEvaluation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	columns := []*vector.Vector{vector.NewVec(types.T_json.ToType()), vector.NewVec(types.T_json.ToType())}
	defer columns[0].Free(mp)
	defer columns[1].Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	for _, column := range columns {
		require.NoError(t, vector.AppendByteJson(column, doc, false, mp))
	}
	columns[1].GetBytesAt(0)[0] = 0xfd
	before := mp.CurrNB()
	strict := NewJSONReadSet([]int{0, 1})
	require.Error(t, strict.PublishMutableColumns(columns, mp))
	require.Nil(t, strict.Readers())
	strict.Close()
	require.Equal(t, before, mp.CurrNB())
	deferred := NewJSONReadSet([]int{0, 1})
	require.NoError(t, deferred.PublishMutableColumnsForEvaluation(columns, mp))
	require.Nil(t, deferred.Readers(), "no partial qualification may survive")
	deferred.Close()
	require.Equal(t, before, mp.CurrNB())
}
