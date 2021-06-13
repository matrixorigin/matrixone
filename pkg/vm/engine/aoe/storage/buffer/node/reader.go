package node

import (
	"context"
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"os"
	"path/filepath"
	// log "github.com/sirupsen/logrus"
)

const (
	NODE_READER = "NR"
)

func init() {
	dio.READER_FACTORY.RegisterBuilder(NODE_READER, &NodeReaderBuilder{})
}

type NodeReaderBuilder struct {
}

func (b *NodeReaderBuilder) Build(rf ioif.IReaderFactory, ctx context.Context) ioif.Reader {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}
	var (
		filename string
		file     ldio.IColPartFile
	)
	fileCtx := ctx.Value("file")
	if fileCtx == nil {
		fn := ctx.Value("filename")
		if fn == nil {
			id := handle.GetID()
			filename = e.MakeFilename(dio.READER_FACTORY.Dirname, e.FTTransientNode, MakeNodeFileName(&id), false)
		} else {
			filename = fmt.Sprintf("%v", fn)
		}
	} else {
		file = fileCtx.(ldio.IColPartFile)
	}
	r := &NodeReader{
		Opts:     rf.GetOpts(),
		Dirname:  rf.GetDir(),
		Handle:   handle,
		Filename: filename,
		File:     file,
	}
	return r
}

type NodeReader struct {
	Opts     *e.Options
	Dirname  string
	Handle   iface.INodeHandle
	Filename string
	File     ldio.IColPartFile
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Handle.GetBuffer().GetDataNode()
	if nr.File != nil {
		nr.File.Read(node.Data)
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
	_, err = w.Read(node.Data)
	if err != nil {
		return err
	}
	// nr.Filename = fname
	return err
}
