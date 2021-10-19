package shard

import (
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
)

type IndexId = metadata.LogBatchId
type LogIndex = metadata.LogIndex
type Entry = wal.Entry
