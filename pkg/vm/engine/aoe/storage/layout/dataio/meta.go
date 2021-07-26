package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type FileMeta struct {
	Indices *base.IndicesMeta
}

func NewFileMeta() *FileMeta {
	m := new(FileMeta)
	m.Indices = base.NewIndicesMeta()
	return m
}
