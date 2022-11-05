package checkpoint

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"
)

func TestCkpCheck(t *testing.T) {
	r := &runner{}
	r.storage.entries = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.Less(b.end)
	}, btree.Options{
		NoLocks: true,
	})

	for i := 0; i < 100; i += 10 {
		r.storage.entries.Set(&CheckpointEntry{
			start:    types.BuildTS(int64(i), 0),
			end:      types.BuildTS(int64(i+9), 0),
			state:    ST_Finished,
			location: fmt.Sprintf("loc-%d", i),
		})
	}

	r.storage.entries.Set(&CheckpointEntry{
		start:    types.BuildTS(int64(100), 0),
		end:      types.BuildTS(int64(109), 0),
		state:    ST_Running,
		location: "loc-100",
	})

	loc, e := r.CollectCheckpointsInRange(types.BuildTS(4, 0), types.BuildTS(5, 0))
	assert.True(t, e.Equal(types.BuildTS(9, 0)))
	assert.Equal(t, "loc-0", loc)

	loc, e = r.CollectCheckpointsInRange(types.BuildTS(12, 0), types.BuildTS(25, 0))
	assert.True(t, e.Equal(types.BuildTS(29, 0)))
	assert.Equal(t, "loc-10;loc-20", loc)
}
