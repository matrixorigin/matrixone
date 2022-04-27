package catalog

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/sirupsen/logrus"
)

type DBEntry struct {
	// *BaseEntry
	*BaseEntry
	catalog *Catalog
	name    string

	entries   map[uint64]*common.DLNode
	nameNodes map[string]*nodeList
	link      *common.Link

	nodesMu sync.RWMutex
}

func NewDBEntry(catalog *Catalog, name string, txnCtx txnif.AsyncTxn) *DBEntry {
	id := catalog.NextDB()
	e := &DBEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CurrOp: OpCreate,
				Txn:    txnCtx,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		catalog:   catalog,
		name:      name,
		entries:   make(map[uint64]*common.DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(common.Link),
	}
	return e
}

func (e *DBEntry) Compare(o common.NodePayload) int {
	oe := o.(*DBEntry).BaseEntry
	return e.DoCompre(oe)
}

func (e *DBEntry) GetName() string { return e.name }

func (e *DBEntry) String() string {
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntry.String(), e.name)
}

func (e *DBEntry) MakeTableIt(reverse bool) *common.LinkIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewLinkIt(e.RWMutex, e.link, reverse)
}

func (e *DBEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.String())
	if level == common.PPL0 {
		return s
	}
	var body string
	it := e.MakeTableIt(true)
	for it.Valid() {
		table := it.Get().GetPayload().(*TableEntry)
		if len(body) == 0 {
			body = table.PPString(level, depth+1, "")
		} else {
			body = fmt.Sprintf("%s\n%s", body, table.PPString(level, depth+1, ""))
		}
		it.Next()
	}

	if len(body) == 0 {
		return s
	}
	return fmt.Sprintf("%s\n%s", s, body)
}

func (e *DBEntry) GetTableEntryByID(id uint64) (table *TableEntry, err error) {
	e.RLock()
	defer e.RUnlock()
	node := e.entries[id]
	if node == nil {
		return nil, ErrNotFound
	}
	table = node.GetPayload().(*TableEntry)
	return
}

func (e *DBEntry) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) *common.DLNode {
	node := e.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetTableNodeLocked(txnCtx)
}

func (e *DBEntry) GetTableEntry(name string, txnCtx txnif.AsyncTxn) (entry *TableEntry, err error) {
	e.RLock()
	n := e.txnGetNodeByNameLocked(name, txnCtx)
	e.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	entry = n.GetPayload().(*TableEntry)
	return
}

func (e *DBEntry) DropTableEntry(name string, txnCtx txnif.AsyncTxn) (deleted *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	dn := e.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.GetPayload().(*TableEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txnCtx txnif.AsyncTxn, dataFactory TableDataFactory) (created *TableEntry, err error) {
	e.Lock()
	created = NewTableEntry(e, schema, txnCtx, dataFactory)
	err = e.addEntryLocked(created)
	e.Unlock()

	return created, err
}

func (e *DBEntry) RemoveEntry(table *TableEntry) error {
	logrus.Infof("Removing: %s", table.String())
	e.Lock()
	defer e.Unlock()
	if n, ok := e.entries[table.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := e.nameNodes[table.GetSchema().Name]
		nn.DeleteNode(table.GetID())
		e.link.Delete(n)
	}
	return nil
}

func (e *DBEntry) addEntryLocked(table *TableEntry) error {
	nn := e.nameNodes[table.schema.Name]
	if nn == nil {
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n

		nn := newNodeList(e, &e.nodesMu, table.schema.Name)
		e.nameNodes[table.schema.Name] = nn

		nn.CreateNode(table.GetID())
	} else {
		node := nn.GetTableNode()
		record := node.GetPayload().(*TableEntry)
		record.RLock()
		err := record.PrepareWrite(table.GetTxn(), record.RWMutex)
		if err != nil {
			record.RUnlock()
			return err
		}
		if record.HasActiveTxn() {
			if !record.IsDroppedUncommitted() {
				record.RUnlock()
				return ErrDuplicate
			}
		} else if !record.HasDropped() {
			record.RUnlock()
			return ErrDuplicate
		}
		record.RUnlock()
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return nil
}

func (e *DBEntry) MakeCommand(id uint32) (txnif.TxnCmd, error) {
	cmdType := CmdCreateDatabase
	e.RLock()
	defer e.RUnlock()
	if e.CurrOp == OpSoftDelete {
		cmdType = CmdDropDatabase
	}
	return newDBCmd(id, cmdType, e), nil
}

func (e *DBEntry) GetCatalog() *Catalog { return e.catalog }

func (e *DBEntry) RecurLoop(processor Processor) (err error) {
	tableIt := e.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload().(*TableEntry)
		if err = processor.OnTable(table); err != nil {
			if err == ErrStopCurrRecur {
				err = nil
				continue
			}
			break
		}
		if err = table.RecurLoop(processor); err != nil {
			return
		}
		tableIt.Next()
	}
	if err == ErrStopCurrRecur {
		err = nil
	}
	return err
}
