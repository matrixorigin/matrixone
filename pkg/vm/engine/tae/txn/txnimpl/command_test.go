package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAppendCmd_MarshalUnmarshal_RoundTrip(t *testing.T) {
	bat := containers.NewBatch()
	defer bat.Close()

	cmd := NewEmptyAppendCmd()
	cmd.Data = bat
	cmd.Ts = types.BuildTS(1, 1)

	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Use BuildCommandFrom which handles the header
	cmdInterface, err := txnbase.BuildCommandFrom(buf)
	require.NoError(t, err)
	cmd2, ok := cmdInterface.(*AppendCmd)
	require.True(t, ok)
	assert.Equal(t, cmd.Ts, cmd2.Ts)
}
