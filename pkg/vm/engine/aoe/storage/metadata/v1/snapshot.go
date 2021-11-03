package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type SSWriter interface {
	PrepareWrite() error
	CommitWrite() error
}

type SSLoader interface {
	PrepareLoad() error
	CommitLoad() error
}

type Snapshoter interface {
	SSWriter
	SSLoader
}

type shardSnapshoter struct {
	dir            string
	name           string
	view           *catalogLogEntry
	shardId, index uint64
	tranId         uint64
	catalog        *Catalog
}

func NewShardSSWriter(catalog *Catalog, dir string, shardId, index uint64) *shardSnapshoter {
	ss := &shardSnapshoter{
		catalog: catalog,
		dir:     dir,
		shardId: shardId,
		index:   index,
	}
	return ss
}

func NewShardSSLoader(catalog *Catalog, name string) *shardSnapshoter {
	ss := &shardSnapshoter{
		catalog: catalog,
		name:    name,
	}
	return ss
}

func (ss *shardSnapshoter) PrepareWrite() error {
	ss.view = ss.catalog.ShardView(ss.shardId, ss.index)
	return nil
}

func (ss *shardSnapshoter) makeName() string {
	return filepath.Join(ss.dir, fmt.Sprintf("%d-%d-%d.meta", ss.shardId, ss.index, time.Now().UTC().UnixMicro()))
}

func (ss *shardSnapshoter) CommitWrite() error {
	ss.name = ss.makeName()
	f, err := os.Create(ss.name)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ss.view.Marshal()
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	logutil.Infof("%s | Shard SS | Flushed", f.Name())
	return err
}

func (ss *shardSnapshoter) ReAllocId(allocator *Sequence, view *Catalog) error {
	ss.tranId = allocator.NextUncommitId()
	processor := newReAllocIdProcessor(allocator, ss.tranId)
	return view.RecurLoop(processor)
}

func (ss *shardSnapshoter) PrepareLoad() error {
	f, err := os.OpenFile(ss.name, os.O_RDONLY, 666)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	mnode := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(mnode)
	ss.view = &catalogLogEntry{}
	if _, err = f.Read(mnode.Buf[:size]); err != nil {
		return err
	}
	if err = ss.view.Unmarshal(mnode.Buf[:size]); err != nil {
		return err
	}
	return ss.ReAllocId(&ss.catalog.Sequence, ss.view.Catalog)
}

func (ss *shardSnapshoter) CommitLoad() error {
	return ss.catalog.SimpleReplaceShard(ss.view, ss.tranId)
}
