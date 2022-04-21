package data

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type TableHandle interface {
	GetAppender() (BlockAppender, error)
	SetAppender(*common.ID) BlockAppender
}

type Table interface {
	GetHandle() TableHandle
	ApplyHandle(TableHandle)
}
