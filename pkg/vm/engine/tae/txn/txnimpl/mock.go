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

package txnimpl

//type mockRelation struct {
//	*txnbase.TxnRelation
//	entry *catalog.TableEntry
//	id    uint64
//}
//
//func mockTestRelation(id uint64, schema *catalog.Schema) *mockRelation {
//	entry := catalog.MockStaloneTableEntry(id, schema)
//	return &mockRelation{
//		TxnRelation: new(txnbase.TxnRelation),
//		id:          id,
//		entry:       entry,
//	}
//}
//
//func (rel *mockRelation) GetID() uint64 { return rel.id }
//func (rel *mockRelation) GetMeta() any  { return rel.entry }
