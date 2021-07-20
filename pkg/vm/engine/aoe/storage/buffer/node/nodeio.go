package node

import (
	"fmt"
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	// log "github.com/sirupsen/logrus"
)

func NewNodeIOWithReader(nh iface.INodeHandle, reader io.Reader) ioif.IO {
	nio := &dio.DefaultIO{}
	nio.Reader = NewNodeReader(nh, "", "", reader)
	return nio
}

func NewNodeIO(nh iface.INodeHandle, dir []byte) ioif.IO {
	nio := &dio.DefaultIO{}
	id := nh.GetID()
	filename := e.MakeFilename(string(dir), e.FTTransientNode, fmt.Sprintf("%d", id), false)
	nio.Reader = NewNodeReader(nh, "", filename, nil)
	nio.Writer = NewNodeWriter(nh, "", filename)
	nio.Cleaner = NewNodeCleaner(filename)
	return nio
}
