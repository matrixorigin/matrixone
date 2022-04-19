package updates

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMutationControllerAppend(t *testing.T) {
	mc := NewMutationNode(nil)

	type appendNode struct {
		commitTS uint64
		rows uint32
	}

	nodeCnt := 1000
	rowsPerNode := uint32(500)
	nodes := make([]appendNode, 0)
	ts := uint64(2)
	for i := 0; i < nodeCnt; i++ {
		nodes = append(nodes, appendNode{
			commitTS: ts,
			rows:     rowsPerNode,
		})
		ts += 2
	}

	st := time.Now()
	queryTS := uint64(1)
	for i, node := range nodes {
		mc.UpdateVisibility(node.rows, node.commitTS)
		if queryTS != 1 {
			if queryTS != 3 {
				prevTS := queryTS
				rows := mc.FetchMaxVisibleRow(prevTS)
				require.Equal(t, uint32(i) * rowsPerNode - 1, rows)
				visi := mc.IsVisible(uint32(i) * rowsPerNode - 200, prevTS)
				require.True(t, visi)
				visi = mc.IsVisible(uint32(i) * rowsPerNode, prevTS)
				require.False(t, visi)
			}
			nextTS := queryTS + 2
			rows := mc.FetchMaxVisibleRow(nextTS)
			require.Equal(t, uint32(i + 1) * rowsPerNode - 1, rows)
			//rows = mc.FetchMaxVisibleRow(queryTS)
			//require.Equal(t, uint32(i) * rowsPerNode - 1, rows)
			visi := mc.IsVisible(uint32(i + 1) * rowsPerNode - 1, nextTS)
			require.True(t, visi)
			//visi = mc.IsVisible(uint32(i) * rowsPerNode - 1, queryTS)
			//require.True(t, visi)
		} else {
			require.Panics(t, func() {
				mc.FetchMaxVisibleRow(queryTS)
			})
		}
		queryTS += 2
	}
	t.Log(time.Since(st).Microseconds(), " us -- 1000 ops")
}
