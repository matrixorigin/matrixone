package logtail

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

type testRow struct {
	id int
}

func createBlockFn[R any]() *TimedSliceBlock[R] {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return NewTimedSliceBlock[R](ts)
}

func TestAOT(t *testing.T) {
	aot := NewAOT[
		*TimedSliceBlock[*testRow],
		*testRow](
		10,
		createBlockFn[*testRow],
		func(a, b *TimedSliceBlock[*testRow]) bool {
			return a.BornTS.Less(b.BornTS)
		})
	for i := 0; i < 30; i++ {
		row := &testRow{id: i}
		err := aot.AppendRow(row)
		assert.NoError(t, err)
	}
	t.Log(aot.BlockCount())
}
