package node

import (
	"context"
	// log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
)

func NewNodeIO(opts *e.Options, ctx context.Context) ioif.IO {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}

	segmentFile := ctx.Value("segmentfile")
	if segmentFile == nil {
		id := handle.GetID()
		filename := e.MakeFilename(dio.WRITER_FACTORY.Dirname, e.FTTransientNode, id.ToPartFileName(), false)
		ctx = context.WithValue(ctx, "filename", filename)
	}

	iof := dio.NewIOFactory(dio.WRITER_FACTORY, dio.READER_FACTORY, dio.CLEANER_FACTORY)
	nio := iof.MakeIO(NODE_WRITER, NODE_READER, NODE_CLEANER, ctx)
	return nio
}
