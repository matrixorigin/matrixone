package merge

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"

type tombstonePolicy struct {
	tombstones []*catalog.ObjectEntry
}

func newTombstonePolicy() *tombstonePolicy {
	return &tombstonePolicy{
		tombstones: make([]*catalog.ObjectEntry, 0),
	}
}

func (m *tombstonePolicy) onObject(obj *catalog.ObjectEntry) {
	if obj.IsTombstone {
		m.tombstones = append(m.tombstones, obj)
	}
}

func (m *tombstonePolicy) revise(int64, int64, *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.tombstones) < 2 {
		return nil, TaskHostDN
	}
	if len(m.tombstones) > 1 {
		return m.tombstones, TaskHostDN
	}
	return nil, TaskHostDN
}

func (m *tombstonePolicy) resetForTable(*catalog.TableEntry) {
	m.tombstones = m.tombstones[:0]
}
