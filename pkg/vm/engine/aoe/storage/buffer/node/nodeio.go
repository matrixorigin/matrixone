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
	nio.Reader = NewNodeReader(nh, nil, reader)
	return nio
}

func NewNodeIO(nh iface.INodeHandle, dir []byte) ioif.IO {
	nio := &dio.DefaultIO{}
	id := nh.GetID()
	filename := e.MakeFilename(string(dir), e.FTTransientNode, fmt.Sprintf("%d", id), false)
	buf := []byte(filename)
	nio.Reader = NewNodeReader(nh, buf, nil)
	nio.Writer = NewNodeWriter(nh, buf)
	nio.Cleaner = NewNodeCleaner(buf)
	return nio
}
