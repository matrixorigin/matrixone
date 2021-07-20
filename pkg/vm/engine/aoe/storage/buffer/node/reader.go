package node

import (
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	"path/filepath"
	// log "github.com/sirupsen/logrus"
)

type NodeReader struct {
	Handle   iface.INodeHandle
	Filename string
	Reader   io.Reader
}

func NewNodeReader(nh iface.INodeHandle, dirname, filename string, reader io.Reader) ioif.Reader {
	nr := &NodeReader{
		Handle: nh,
		Reader: reader,
	}

	if nr.Reader == nil {
		if filename != "" {
			nr.Filename = filename
		} else {
			id := nh.GetID()
			nr.Filename = e.MakeFilename(dirname, e.FTTransientNode, MakeNodeFileName(id), false)
		}
	}
	return nr
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Handle.GetBuffer().GetDataNode()
	if nr.Reader != nil {
		node.ReadFrom(nr.Reader)
		return nil
	}

	dir := filepath.Dir(nr.Filename)
	// log.Info(dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(nr.Filename, os.O_RDONLY, 0666)
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
