// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func init() {
	MoDatabaseTableDefs = make([]engine.TableDef, len(MoDatabaseSchema))
	for i, name := range MoDatabaseSchema {
		MoDatabaseTableDefs[i] = newAttributeDef(name, MoDatabaseTypes[i], i == 0)
	}
	MoTablesTableDefs = make([]engine.TableDef, len(MoTablesSchema))
	for i, name := range MoTablesSchema {
		MoTablesTableDefs[i] = newAttributeDef(name, MoTablesTypes[i], i == 0)
	}
	MoColumnsTableDefs = make([]engine.TableDef, len(MoColumnsSchema))
	for i, name := range MoColumnsSchema {
		MoColumnsTableDefs[i] = newAttributeDef(name, MoColumnsTypes[i], i == 0)
	}
}

func newAttributeDef(name string, typ types.Type, isPrimary bool) engine.TableDef {
	return &engine.AttributeDef{
		Attr: engine.Attribute{
			Type:    typ,
			Name:    name,
			Primary: isPrimary,
			Alg:     compress.Lz4,
		},
	}
}
