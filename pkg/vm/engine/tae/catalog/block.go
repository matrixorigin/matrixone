package catalog

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type BlockDataFactory = func(meta *BlockEntry) data.Block

type BlockEntry struct {
	*BaseEntry
	segment *SegmentEntry
	state   EntryState
	blkData data.Block
}

func NewEmptyBlockEntry() *BlockEntry {
	return &BlockEntry{
		BaseEntry: new(BaseEntry),
	}
}

func NewBlockEntry(segment *SegmentEntry, txn txnif.AsyncTxn, state EntryState, dataFactory BlockDataFactory) *BlockEntry {
	id := segment.GetTable().GetDB().catalog.NextBlock()
	e := &BlockEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		segment: segment,
		state:   state,
	}
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	return e
}

func (entry *BlockEntry) GetCatalog() *Catalog { return entry.segment.table.db.catalog }

func (entry *BlockEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *BlockEntry) GetSegment() *SegmentEntry {
	return entry.segment
}

func (entry *BlockEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdCreateBlock
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropBlock
	}
	return newBlockCmd(id, cmdType, entry), nil
}

func (entry *BlockEntry) Compare(o common.NodePayload) int {
	oe := o.(*BlockEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *BlockEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringLocked())
	return s
}

func (entry *BlockEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *BlockEntry) StringLocked() string {
	return fmt.Sprintf("[%s]BLOCK%s", entry.state.Repr(), entry.BaseEntry.String())
}

func (entry *BlockEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetSegment().GetTable().GetID(),
		SegmentID: entry.GetSegment().GetID(),
		BlockID:   entry.GetID(),
	}
}

func (entry *BlockEntry) GetBlockData() data.Block { return entry.blkData }
func (entry *BlockEntry) GetSchema() *Schema       { return entry.GetSegment().GetTable().GetSchema() }

func (entry *BlockEntry) PrepareRollback() (err error) {
	entry.RLock()
	currOp := entry.CurrOp
	entry.RUnlock()
	if err = entry.BaseEntry.PrepareRollback(); err != nil {
		return
	}
	if currOp == OpCreate {
		err = entry.GetSegment().RemoveEntry(entry)
	}
	return
}

func (entry *BlockEntry) WriteTo(w io.Writer) (err error) {
	if err = entry.BaseEntry.WriteTo(w); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, entry.state); err != nil {
		return
	}
	return
}

func (entry *BlockEntry) ReadFrom(r io.Reader) (err error) {
	if err = entry.BaseEntry.ReadFrom(r); err != nil {
		return
	}
	return binary.Read(r, binary.BigEndian, &entry.state)
}

func (entry *BlockEntry) MakeLogEntry() *EntryCommand {
	return newBlockCmd(0, CmdLogBlock, entry)
}

func (entry *BlockEntry) Clone() CheckpointItem {
	cloned := &BlockEntry{
		BaseEntry: entry.BaseEntry.Clone(),
		state:     entry.state,
		segment:   entry.segment,
	}
	return cloned
}

func (entry *BlockEntry) CloneCreate() CheckpointItem {
	cloned := &BlockEntry{
		BaseEntry: entry.BaseEntry.CloneCreate(),
		state:     entry.state,
		segment:   entry.segment,
	}
	return cloned
}
