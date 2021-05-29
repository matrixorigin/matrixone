package node

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"os"
	"path/filepath"
)

const (
	NODE_WRITER = "NW"
)

func init() {
	dio.WRITER_FACTORY.RegisterBuilder(NODE_WRITER, &NodeWriterBuilder{})
}

type NodeWriterBuilder struct {
}

func (b *NodeWriterBuilder) Build(wf ioif.IWriterFactory, ctx context.Context) ioif.Writer {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}
	var filename string
	fn := ctx.Value("filename")
	if fn == nil {
		id := handle.GetID()
		filename = e.MakeFilename(dio.READER_FACTORY.Dirname, e.FTTransientNode, MakeNodeFileName(&id), false)
	} else {
		filename = fmt.Sprintf("%v", fn)
	}
	w := &NodeWriter{
		Opts:     wf.GetOpts(),
		Dirname:  wf.GetDir(),
		Handle:   handle,
		Filename: filename,
	}
	return w
}

type NodeWriter struct {
	Opts     *e.Options
	Dirname  string
	Handle   iface.INodeHandle
	Filename string
}

func MakeNodeFileName(id *layout.ID) string {
	return fmt.Sprintf("%d_%d_%d_%d", id.TableID, id.SegmentID, id.BlockID, id.PartID)
}

func (sw *NodeWriter) Flush() (err error) {
	node := sw.Handle.GetBuffer().GetDataNode()
	dir := filepath.Dir(sw.Filename)
	log.Info(dir)
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
	_, err = w.Write(node.Data)
	if err != nil {
		return err
	}
	return err
}
