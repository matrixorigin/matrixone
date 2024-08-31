package merge

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type objSizePolicy struct {
	objects []*catalog.ObjectEntry
}

func newObjSizePolicy() *objSizePolicy {
	return &objSizePolicy{
		objects: make([]*catalog.ObjectEntry, 0),
	}
}

func (m *objSizePolicy) onObject(obj *catalog.ObjectEntry) {
	if !obj.IsTombstone {
		m.objects = append(m.objects, obj)
	}
}

func (m *objSizePolicy) revise(cpu, mem int64, config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.objects) < 2 {
		return nil, TaskHostDN
	}
	objs, taskHostKind := m.reviseDataObjs(config)
	if len(objs) > 1 {
		return objs, taskHostKind
	}
	return nil, taskHostKind
}

func (m *objSizePolicy) reviseDataObjs(config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	n := 0
	for _, obj := range m.objects {
		if obj.OriginSize() < config.ObjectMinOsize {
			n++
		}
	}
	if n < 5 {
		return nil, TaskHostDN
	}
	size := min(config.MergeMaxOneRun, n)

	revisedObj := make([]*catalog.ObjectEntry, 0, size)
	for _, obj := range m.objects {
		if len(revisedObj) == size {
			break
		}
		if obj.OriginSize() < config.ObjectMinOsize {
			revisedObj = append(revisedObj, obj)
		}
	}
	return revisedObj, TaskHostDN
}

func (m *objSizePolicy) resetForTable(*catalog.TableEntry) {
	m.objects = m.objects[:0]
}
