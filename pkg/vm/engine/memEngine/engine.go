package memEngine

import (
	"fmt"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
)

func New(db *kv.KV, n engine.Node) *MemEngine {
	return &MemEngine{
		n:  n,
		db: db,
	}
}

func (e *MemEngine) Delete(_ uint64, _ string, _ engine.Snapshot) error {
	return nil
}

func (e *MemEngine) Create(_ uint64, _ string, _ int, _ engine.Snapshot) error {
	return nil
}

func (e *MemEngine) Databases(_ engine.Snapshot) []string {
	return []string{"test"}
}

func (e *MemEngine) Database(name string, _ engine.Snapshot) (engine.Database, error) {
	if name != "test" {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{db: e.db, n: e.n}, nil
}

func (e *MemEngine) Node(_ string, _ engine.Snapshot) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}

func (e *MemEngine) DefaultDatabase() string {
	return "test"
}

func (e *MemEngine) DatabaseExists(name string) bool {
	return name == "test"
}

func (e *MemEngine) Resolve(_ string, tableName string) (*plan.ObjectRef, *plan.TableDef) {
	schemaName := "test"
	db, err := e.Database(schemaName, nil)
	if err != nil {
		panic(err)
	}
	rel, err := db.Relation(tableName, nil)
	if err != nil {
		panic(err)
	}
	defs := rel.TableDefs(nil)
	cols := make([]*plan.ColDef, 0, len(defs))
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		cols = append(cols, &plan.ColDef{
			Name: attr.Attr.Name,
			Typ: &plan.Type{
				Id:    plan.Type_TypeId(attr.Attr.Type.Oid),
				Width: attr.Attr.Type.Width,
				Size:  attr.Attr.Type.Size,
			},
		})
	}
	return &plan.ObjectRef{SchemaName: schemaName, ObjName: schemaName}, &plan.TableDef{Name: tableName, Cols: cols}
}

func (e *MemEngine) ResolveVariable(_ string, _, _ bool) (interface{}, error) {
	return nil, nil
}

func (e *MemEngine) GetPrimaryKeyDef(_ string, _ string) []*plan.ColDef {
	return nil
}

func (e *MemEngine) GetHideKeyDef(_ string, _ string) *plan.ColDef {
	return nil
}

func (e *MemEngine) Cost(_ *plan.ObjectRef, _ *plan.Expr) *plan.Cost {
	return &plan.Cost{}
}
