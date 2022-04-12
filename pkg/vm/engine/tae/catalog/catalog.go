package catalog

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/sirupsen/logrus"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+

type Catalog struct {
	*IDAlloctor
	// sm.ClosedState
	// sm.StateMachine
	*sync.RWMutex
	store store.Store

	entries   map[uint64]*common.DLNode
	nameNodes map[string]*nodeList
	link      *common.Link

	nodesMu  sync.RWMutex
	commitMu sync.RWMutex
}

func MockCatalog(dir, name string, cfg *store.StoreCfg) *Catalog {
	var driver store.Store
	var err error
	driver, err = store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		store:      driver,
		entries:    make(map[uint64]*common.DLNode),
		nameNodes:  make(map[string]*nodeList),
		link:       new(common.Link),
	}
	// catalog.StateMachine.Start()
	return catalog
}

func (catalog *Catalog) Close() error {
	if catalog.store != nil {
		catalog.store.Close()
	}
	return nil
}

func (catalog *Catalog) GetDatabaseByID(id uint64) (db *DBEntry, err error) {
	catalog.RLock()
	defer catalog.RUnlock()
	node := catalog.entries[id]
	if node == nil {
		err = ErrNotFound
		return
	}
	db = node.GetPayload().(*DBEntry)
	return
}

func (catalog *Catalog) addEntryLocked(database *DBEntry) error {
	nn := catalog.nameNodes[database.name]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n

		nn := newNodeList(catalog, &catalog.nodesMu, database.name)
		catalog.nameNodes[database.name] = nn

		nn.CreateNode(database.GetID())
	} else {
		node := nn.GetDBNode()
		record := node.GetPayload().(*DBEntry)
		record.RLock()
		err := record.PrepareWrite(database.GetTxn(), record.RWMutex)
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
			logrus.Info(record.String())
			return ErrDuplicate
		}

		record.RUnlock()
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n
		nn.CreateNode(database.GetID())
	}
	return nil
}

func (catalog *Catalog) MakeDBIt(reverse bool) *common.LinkIt {
	return common.NewLinkIt(catalog.RWMutex, catalog.link, reverse)
}

func (catalog *Catalog) SimplePPString(level common.PPLevel) string {
	return catalog.PPString(level, 0, "")
}

func (catalog *Catalog) PPString(level common.PPLevel, depth int, prefix string) string {
	cnt := 0
	var body string
	it := catalog.MakeDBIt(true)
	for it.Valid() {
		cnt++
		table := it.Get().GetPayload().(*DBEntry)
		if len(body) == 0 {
			body = table.PPString(level, depth+1, "")
		} else {
			body = fmt.Sprintf("%s\n%s", body, table.PPString(level, depth+1, ""))
		}
		it.Next()
	}

	head := fmt.Sprintf("CATALOG[CNT=%d]", cnt)

	if len(body) == 0 {
		return head
	}
	return fmt.Sprintf("%s\n%s", head, body)
}

func (catalog *Catalog) RemoveEntry(database *DBEntry) error {
	// logrus.Infof("Removing: %s", database.String())
	catalog.Lock()
	defer catalog.Unlock()
	if n, ok := catalog.entries[database.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := catalog.nameNodes[database.name]
		nn.DeleteNode(database.GetID())
		catalog.link.Delete(n)
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) *common.DLNode {
	node := catalog.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetDBNodeLocked(txnCtx)
}

func (catalog *Catalog) GetDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	catalog.RLock()
	n := catalog.txnGetNodeByNameLocked(name, txnCtx)
	catalog.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	return n.GetPayload().(*DBEntry), nil
}

func (catalog *Catalog) DropDBEntry(name string, txnCtx txnif.AsyncTxn) (deleted *DBEntry, err error) {
	catalog.Lock()
	defer catalog.Unlock()
	dn := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.GetPayload().(*DBEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	// node := catalog.txnGetNodeByNameLocked(name, txnCtx)
	// if node != nil {
	// 	record := node.payload.(*DBEntry)
	// 	record.RLock()
	// 	defer record.RUnlock()
	// 	if record.Txn != nil {
	// 		if record.Txn.GetID() == txnCtx.GetID() {
	// 			if !record.IsDroppedUncommitted() {
	// 				err = ErrDuplicate
	// 			}
	// 		} else {
	// 			err = txnif.TxnWWConflictErr
	// 		}
	// 	} else {
	// 		if !record.HasDropped() {
	// 			err = ErrDuplicate
	// 		}
	// 	}
	// 	if err != nil {
	// 		catalog.Unlock()
	// 		return nil, err
	// 	}
	// }
	entry := NewDBEntry(catalog, name, txnCtx)
	err = catalog.addEntryLocked(entry)
	catalog.Unlock()

	return entry, err
}
