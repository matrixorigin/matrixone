package logtail

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

type testRows struct {
	id int
}

func (r *testRows) Length() int               { return 1 }
func (r *testRows) Window(_, _ int) *testRows { return nil }

func createBlockFn[R any]() *TimedSliceBlock[R] {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return NewTimedSliceBlock[R](ts)
}

func TestAOT(t *testing.T) {
	aot := NewAOT[
		*TimedSliceBlock[*testRows],
		*testRows](
		10,
		createBlockFn[*testRows],
		func(a, b *TimedSliceBlock[*testRows]) bool {
			return a.BornTS.Less(b.BornTS)
		})
	for i := 0; i < 30; i++ {
		rows := &testRows{id: i}
		err := aot.Append(rows)
		assert.NoError(t, err)
	}
	t.Log(aot.BlockCount())
}
