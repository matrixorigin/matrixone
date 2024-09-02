// Copyright 2021 Matrix Origin
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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type DataFactory struct {
	dir string
	rt  *dbutils.Runtime
}

func NewDataFactory(
	rt *dbutils.Runtime, dir string,
) *DataFactory {
	return &DataFactory{
		dir: dir,
		rt:  rt,
	}
}

func (factory *DataFactory) MakeTableFactory() catalog.TableDataFactory {
	return func(meta *catalog.TableEntry) data.Table {
		return newTable(meta, factory.rt)
	}
}

func (factory *DataFactory) MakeObjectFactory() catalog.ObjectDataFactory {
	return func(meta *catalog.ObjectEntry) data.Object {
		if meta.IsAppendable() {
			return newAObject(meta, factory.rt, meta.IsTombstone)
		} else {
			return newObject(meta, factory.rt)
		}
	}
}
