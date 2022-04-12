package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type mockRelation struct {
	*txnbase.TxnRelation
	entry *catalog.TableEntry
	id    uint64
}

func mockTestRelation(id uint64, schema *catalog.Schema) *mockRelation {
	entry := catalog.MockStaloneTableEntry(id, schema)
	return &mockRelation{
		TxnRelation: new(txnbase.TxnRelation),
		id:          id,
		entry:       entry,
	}
}

func (rel *mockRelation) GetID() uint64        { return rel.id }
func (rel *mockRelation) GetMeta() interface{} { return rel.entry }
