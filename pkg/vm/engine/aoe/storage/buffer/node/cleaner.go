package node

import (
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	// log "github.com/sirupsen/logrus"
)

type NodeCleaner struct {
	Filename string
}

func NewNodeCleaner(filename string) ioif.Cleaner {
	return &NodeCleaner{
		Filename: filename,
	}
}

func (nc *NodeCleaner) Clean() error {
	// log.Infof("NodeCleaner removing %s", nc.Filename)
	return os.Remove(nc.Filename)
}
