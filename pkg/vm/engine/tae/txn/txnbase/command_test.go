package txnbase

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Basic round-trip test
func TestComposedCmd_MarshalUnmarshal_RoundTrip(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	cc.AddCmd(cmd1)

	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Use BuildCommandFrom which handles the header
	cmd, err := BuildCommandFrom(buf)
	require.NoError(t, err)
	cc2, ok := cmd.(*ComposedCmd)
	require.True(t, ok)
	assert.Equal(t, len(cc.Cmds), len(cc2.Cmds))
}

// TestTxnCmd_MarshalBinary_Basic tests basic MarshalBinary functionality
func TestTxnCmd_MarshalBinary_Basic(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	// Add a simple command
	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Test that MarshalBinary works and uses sync.Pool
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should contain data")

	// Test multiple calls work correctly
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, buf, buf2, "multiple calls should produce same result")
}
