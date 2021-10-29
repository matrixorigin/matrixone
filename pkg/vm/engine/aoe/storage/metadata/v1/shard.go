package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type ShardSnapshot struct {
	Dir            string
	Name           string
	View           *catalogLogEntry
	ShardId, Index uint64
	Catalog        *Catalog
}

func NewShardSnapshotWriter(catalog *Catalog, dir string, shardId, index uint64) *ShardSnapshot {
	ss := &ShardSnapshot{
		Catalog: catalog,
		Dir:     dir,
		ShardId: shardId,
		Index:   index,
	}
	return ss
}

func NewShardSnapshotReader(catalog *Catalog, name string) *ShardSnapshot {
	ss := &ShardSnapshot{
		Catalog: catalog,
		Name:    name,
	}
	return ss
}

func (ss *ShardSnapshot) PrepareWrite() error {
	ss.View = ss.Catalog.ShardView(ss.ShardId, ss.Index)
	return nil
}

func (ss *ShardSnapshot) makeName() string {
	return filepath.Join(ss.Dir, fmt.Sprintf("%d-%d-%d.meta", ss.ShardId, ss.Index, time.Now().UnixMicro()))
}

func (ss *ShardSnapshot) CommitWrite() error {
	f, err := os.Create(ss.makeName())
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ss.View.Marshal()
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

func (ss *ShardSnapshot) PrepareRead() error {
	f, err := os.OpenFile(ss.Name, os.O_RDONLY, 666)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	buf := make([]byte, int(info.Size()))
	ss.View = &catalogLogEntry{}
	if _, err = f.Read(buf); err != nil {
		return err
	}
	if err = ss.View.Unmarshal(buf); err != nil {
		return err
	}
	return nil
}

func (ss *ShardSnapshot) ApplyRead() error {
	// TODO
	return nil
}

type shardNode struct {
	common.SSLLNode
	Catalog *Catalog
	id      uint64
}

func newShardNode(catalog *Catalog, shardId uint64) *shardNode {
	return &shardNode{
		SSLLNode: *common.NewSSLLNode(),
		Catalog:  catalog,
		id:       shardId,
	}
}

func (n *shardNode) CreateNode(epoch uint64) *groupNode {
	nn := newGroupNode(n.id, epoch)
	n.Catalog.nodesMu.Lock()
	defer n.Catalog.nodesMu.Unlock()
	n.Insert(nn)
	return nn
}

func (n *shardNode) GetGroup() *groupNode {
	n.Catalog.nodesMu.RLock()
	defer n.Catalog.nodesMu.RUnlock()
	return n.GetNext().(*groupNode)
}

func (n *shardNode) PString(level PPLevel) string {
	curr := n.GetNext()
	group := curr.(*groupNode)
	s := fmt.Sprintf("ShardNode<%d>->[%s]", n.id, group.String())
	curr = curr.GetNext()
	for curr != nil {
		group = curr.(*groupNode)
		s = fmt.Sprintf("%s->[%s]", s, group.String())
		curr = curr.GetNext()
	}
	return s
}

type groupNode struct {
	common.SSLLNode
	id, epoch uint64
	ids       map[uint64]bool
}

func newGroupNode(id, epoch uint64) *groupNode {
	return &groupNode{
		SSLLNode: *common.NewSSLLNode(),
		id:       id,
		epoch:    epoch,
		ids:      make(map[uint64]bool),
	}
}

func (n *groupNode) Add(id uint64) error {
	_, ok := n.ids[id]
	if ok {
		return DuplicateErr
	}
	n.ids[id] = true
	return nil
}

func (n *groupNode) Delete(id uint64) error {
	_, ok := n.ids[id]
	if !ok {
		return TableNotFoundErr
	}
	delete(n.ids, id)
	return nil
}

func (n *groupNode) String() string {
	if n == nil {
		return "nil"
	}
	s := fmt.Sprintf("Epoch-%d", n.epoch)
	for id, _ := range n.ids {
		s = fmt.Sprintf("%s %d", s, id)
	}
	return s
}

func (n *groupNode) GetEntry(catalog *Catalog, id uint64) *Table {
	if n == nil {
		return nil
	}
	_, ok := n.ids[id]
	if !ok {
		return nil
	}
	return catalog.TableSet[id]
}
