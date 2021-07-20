package node

import (
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	// log "github.com/sirupsen/logrus"
)

type NodeCleaner struct {
	Filename []byte
}

func NewNodeCleaner(filename []byte) ioif.Cleaner {
	return &NodeCleaner{
		Filename: filename,
	}
}

func (nc *NodeCleaner) Clean() error {
	// log.Infof("NodeCleaner removing %s", nc.Filename)
	return os.Remove(string(nc.Filename))
}
