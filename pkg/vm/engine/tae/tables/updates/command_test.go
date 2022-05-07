package updates

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestCompactBlockCmd(t *testing.T) {
	node := MockAppendNode(341, 2515, nil)
	cmd := NewAppendCmd(1, node)

	var w bytes.Buffer
	err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	checkAppendCmdIsEqual(t, cmd, cmd2.(*UpdateCmd))
}

func checkAppendCmdIsEqual(t *testing.T, cmd1, cmd2 *UpdateCmd) {
	assert.Equal(t, txnbase.CmdAppend, cmd1.GetType())
	assert.Equal(t, txnbase.CmdAppend, cmd2.GetType())
	assert.Equal(t,cmd1.append.maxRow,cmd2.append.maxRow)
}
