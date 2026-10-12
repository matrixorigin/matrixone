// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
)

// Retain the pre-review algorithm only as a reproducible cost baseline.
func legacyReflectCollationAdmission(owner any) error {
	seen := make(map[uintptr]struct{})
	var walk func(reflect.Value) error
	walk = func(v reflect.Value) error {
		if !v.IsValid() {
			return nil
		}
		if v.Kind() == reflect.Interface {
			if v.IsNil() {
				return nil
			}
			return walk(v.Elem())
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			if _, ok := seen[v.Pointer()]; ok {
				return nil
			}
			seen[v.Pointer()] = struct{}{}
			return walk(v.Elem())
		}
		if v.CanInterface() {
			switch value := v.Interface().(type) {
			case Type:
				if err := value.ValidateCollation(); err != nil {
					return err
				}
				if isPlanMySQLStringType(value.Id) {
					return collation.RequireLegacy(value.Charset, value.CollationVersion, 0)
				}
				return nil
			case TableDef:
				if err := collation.RequireLegacy(value.DefaultCharset, value.CollationVersion, value.KeyFormat); err != nil {
					return err
				}
			case IndexDef:
				if err := collation.RequireLegacy(0, 0, value.KeyFormat); err != nil {
					return err
				}
			}
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).PkgPath == "" {
					if err := walk(v.Field(i)); err != nil {
						return err
					}
				}
			}
		case reflect.Slice, reflect.Array:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return nil
			}
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Map:
			it := v.MapRange()
			for it.Next() {
				if err := walk(it.Value()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(reflect.ValueOf(owner))
}

func collationAdmissionBenchmarkPlan(columns int) *Plan {
	typ := Type{Id: 61, Width: 64, Charset: 3}
	table := &TableDef{Name: "admission", DefaultCharset: 3,
		Name2ColIndex: make(map[string]int32, columns),
		Pkey:          &PrimaryKeyDef{PkeyColName: "c0"},
		Indexes:       []*IndexDef{{IndexName: "idx", Parts: []string{"c0"}}},
		Createsql:     "create table admission(c0 varchar(64) primary key)"}
	node := &Node{NodeType: Node_TABLE_SCAN, TableDef: table, ObjRef: &ObjectRef{ObjName: "admission"}, Stats: &Stats{}}
	for i := range columns {
		name := fmt.Sprintf("c%d", i)
		table.Cols = append(table.Cols, &ColDef{Name: name, Typ: typ,
			Default: &Default{Expr: &Expr{Typ: typ, Expr: &Expr_Lit{Lit: &Literal{Value: &Literal_Sval{Sval: "default"}}}}}})
		table.Name2ColIndex[name] = int32(i)
		node.ProjectList = append(node.ProjectList, &Expr{Typ: typ,
			Expr: &Expr_Col{Col: &ColRef{ColPos: int32(i), Name: name}}})
	}
	node.FilterList = []*Expr{{Typ: Type{Id: 10}, Expr: &Expr_F{F: &Function{
		Func: &ObjectRef{ObjName: "="}, Args: []*Expr{node.ProjectList[0], table.Cols[0].Default.Expr}}}}}
	return &Plan{Plan: &Plan_Query{Query: &Query{Nodes: []*Node{node}, Steps: []int32{0}}}}
}

// StatementAdmission measures both retained producer and compile boundaries,
// not ProtoSize as a proxy and not evaluator work unrelated to the fence.
func BenchmarkStatementCollationAdmission(b *testing.B) {
	for _, columns := range []int{8, 32, 128, 512} {
		owner := collationAdmissionBenchmarkPlan(columns)
		for _, mode := range []struct {
			name  string
			admit func(any) error
		}{
			{"before_reflect", legacyReflectCollationAdmission}, {"after_typed", RequireLegacyCollations},
		} {
			b.Run(fmt.Sprintf("cols_%d/%s", columns, mode.name), func(b *testing.B) {
				if err := mode.admit(owner); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := mode.admit(owner); err != nil {
						b.Fatal(err)
					}
					if err := mode.admit(owner); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
