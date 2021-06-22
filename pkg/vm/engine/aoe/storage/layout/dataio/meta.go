package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type FileMeta struct {
	Indexes *base.IndexesMeta
}

func NewFileMeta() *FileMeta {
	m := new(FileMeta)
	m.Indexes = base.NewIndexesMeta()
	return m
}
