package db

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type metaFileCleaner struct {
	info *md.MetaInfo
}

func NewMetaFileCleaner(info *md.MetaInfo) *metaFileCleaner {
	return &metaFileCleaner{
		info: info,
	}
}

func (c *metaFileCleaner) OnExec() {
	handle := NewReplayHandle(c.info.Conf.Dir)
	handle.CleanupWithCtx(3)
}

func (c *metaFileCleaner) OnStopped() {
}
