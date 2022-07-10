package objectio

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type ObjectDir struct {
	common.RefHelper
	nodes map[string]*ObjectFile
	inode *Inode
	fs    *ObjectFS
}
