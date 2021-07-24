package node

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	"path/filepath"
	// log "github.com/sirupsen/logrus"
)

type NodeReader struct {
	Handle   iface.INodeHandle
	Filename []byte
	Reader   io.Reader
}

func NewNodeReader(nh iface.INodeHandle, filename []byte, reader io.Reader) ioif.Reader {
	nr := &NodeReader{
		Handle: nh,
		Reader: reader,
	}

	if nr.Reader == nil {
		nr.Filename = filename
	}
	return nr
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Handle.GetBuffer().GetDataNode()
	if nr.Reader != nil {
		_, err = node.ReadFrom(nr.Reader)
		return err
	}
	filename := string(nr.Filename)
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	_, err = node.ReadFrom(w)
	if err != nil {
		return err
	}
	// nr.Filename = fname
	return err
}
