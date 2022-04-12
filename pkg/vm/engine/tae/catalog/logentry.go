package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type LogEntry = entry.Entry
type LogEntryType = entry.Type

const (
	ETCreateDatabase LogEntryType = iota + entry.ETCustomizedStart
	ETSoftDeleteDatabase
	ETHardDeleteDatabase
	ETCreateTable
	ETSoftDeleteTable
	ETHardDeleteTable
	ETCreateSegment
	ETDropSegment
	ETCreateBlock
	ETDropBlock
	ETTransaction
)
