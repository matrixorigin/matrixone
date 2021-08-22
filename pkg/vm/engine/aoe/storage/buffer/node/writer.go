package node

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/logutil"
	"os"
	"path/filepath"
)

type NodeWriter struct {
	Handle   iface.INodeHandle
	Filename []byte
}

func NewNodeWriter(nh iface.INodeHandle, filename []byte) ioif.Writer {
	w := &NodeWriter{
		Handle:   nh,
		Filename: filename,
	}
	return w
}

func MakeNodeFileName(id uint64) string {
	return fmt.Sprintf("%d", id)
}

func (sw *NodeWriter) Flush() (err error) {
	node := sw.Handle.GetBuffer().GetDataNode()
	filename := string(sw.Filename)
	dir := filepath.Dir(filename)
	logutil.S().Infof(" %s | SpillNode | Flushing", sw.Filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.Create(filename)
	if err != nil {
		return err
	}
	_, err = node.WriteTo(w)
	if err != nil {
		return err
	}
	return err
}
