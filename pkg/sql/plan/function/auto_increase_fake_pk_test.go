// Copyright 2026 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func autoIncrAttr(name string, autoIncr, hidden bool) *engine.AttributeDef {
	return &engine.AttributeDef{Attr: engine.Attribute{Name: name, AutoIncrement: autoIncr, IsHidden: hidden}}
}

// A table without a primary key gets a hidden auto-increment fake pk column.
// It must not be reported as the table's AUTO_INCREMENT column.
func TestGetTableAutoIncrColSkipsFakePrimaryKey(t *testing.T) {
	// no user auto-increment column: only the fake pk
	defs := []engine.TableDef{
		autoIncrAttr("a", false, false),
		autoIncrAttr(catalog.FakePrimaryKeyColName, true, true),
	}
	require.Equal(t, "", getTableAutoIncrCol(defs))

	// fake pk listed before a user auto-increment column (no pk, auto_increment unique key)
	defs = []engine.TableDef{
		autoIncrAttr(catalog.FakePrimaryKeyColName, true, true),
		autoIncrAttr("id", true, false),
	}
	require.Equal(t, "id", getTableAutoIncrCol(defs))

	// ordinary user auto-increment column
	defs = []engine.TableDef{
		autoIncrAttr("id", true, false),
		autoIncrAttr("b", false, false),
	}
	require.Equal(t, "id", getTableAutoIncrCol(defs))

	// views have no AUTO_INCREMENT
	defs = []engine.TableDef{&engine.ViewDef{}, autoIncrAttr("id", true, false)}
	require.Equal(t, "", getTableAutoIncrCol(defs))
}
