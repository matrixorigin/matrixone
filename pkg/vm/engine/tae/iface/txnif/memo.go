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

package txnif

import (
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type TxnMemo struct {
	*common.Tree
	isCatalogChanged bool
}

func NewTxnMemo() *TxnMemo {
	return &TxnMemo{
		Tree: common.NewTree(),
	}
}

func (memo *TxnMemo) AddCatalogChange() {
	memo.isCatalogChanged = true
}

func (memo *TxnMemo) HasAnyTableDataChanges() bool {
	return memo.TableCount() > 0
}

func (memo *TxnMemo) HasTableDataChanges(id uint64) bool {
	return memo.HasTable(id)
}

func (memo *TxnMemo) HasCatalogChanges() bool {
	return memo.isCatalogChanged
}

func (memo *TxnMemo) GetDirtyTableByID(id uint64) *common.TableTree {
	return memo.GetTable(id)
}

func (memo *TxnMemo) GetDirty() *common.Tree {
	return memo.Tree
}

func (memo *TxnMemo) WriteTo(w io.Writer) (n int64, err error) {
	var tmpn int64
	if tmpn, err = memo.Tree.WriteTo(w); err != nil {
		return
	}
	n += tmpn
	isCatalogChanged := int8(0)
	if memo.isCatalogChanged {
		isCatalogChanged = 1
	}
	if err = binary.Write(w, binary.BigEndian, isCatalogChanged); err != nil {
		return
	}
	n += 1
	return
}

func (memo *TxnMemo) ReadFrom(r io.Reader) (n int64, err error) {
	var tmpn int64
	if tmpn, err = memo.Tree.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	isCatalogChanged := int8(0)
	if err = binary.Read(r, binary.BigEndian, &isCatalogChanged); err != nil {
		return
	}
	n += 1
	if isCatalogChanged == 1 {
		memo.isCatalogChanged = true
	}
	return
}
