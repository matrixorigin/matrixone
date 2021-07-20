package node

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	"path/filepath"
)

type NodeWriter struct {
	Handle   iface.INodeHandle
	Filename string
}

func NewNodeWriter(nh iface.INodeHandle, dirname, filename string) ioif.Writer {
	w := &NodeWriter{
		Handle:   nh,
		Filename: filename,
	}
	if w.Filename == "" {
		id := nh.GetID()
		w.Filename = e.MakeFilename(dirname, e.FTTransientNode, MakeNodeFileName(id), false)
	}
	return w
}

func MakeNodeFileName(id uint64) string {
	return fmt.Sprintf("%d", id)
}

func (sw *NodeWriter) Flush() (err error) {
	node := sw.Handle.GetBuffer().GetDataNode()
	dir := filepath.Dir(sw.Filename)
	log.Infof("Flushing %s", sw.Filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(sw.Filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	_, err = node.WriteTo(w)
	if err != nil {
		return err
	}
	return err
}
