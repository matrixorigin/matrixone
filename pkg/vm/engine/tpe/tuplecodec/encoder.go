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

package tuplecodec

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorWrongTenantID = errors.New("wrong tenant id")
)

func NewTupleKeyEncoder(tenantID uint64) *TupleKeyEncoder {
	oe := orderedcodec.NewOrderedEncoder()
	tke := &TupleKeyEncoder{oe : oe}
	tp,_ := tke.EncodeTenantPrefix(nil,tenantID)
	tke.oe = oe
	tke.tenantPrefix = &tp
	return tke
}

// EncodeTenantPrefix encodes the tenant prefix
func (tke *TupleKeyEncoder) EncodeTenantPrefix(prefix TupleKey, tenantID uint64) (TupleKey,*EncodedItem) {
	if tenantID < SystemTenantID {
		panic(errorWrongTenantID)
	}
	if tenantID == SystemTenantID {
		return nil,nil
	}

	pb, _ := tke.oe.EncodeUint64(prefix,tenantID)
	return pb,nil
}

func (tke *TupleKeyEncoder) GetTenantPrefix() TupleKey {
	return *tke.tenantPrefix
}

// EncodeDatabasePrefix encodes the database prefix
func (tke *TupleKeyEncoder) EncodeDatabasePrefix(prefix TupleKey,dbID uint64) (TupleKey,*EncodedItem) {
	pre := append(prefix,tke.GetTenantPrefix()...)
	dbPrefix,_ := tke.oe.EncodeUint64(pre,dbID)
	return dbPrefix,nil
}

// EncodeTablePrefix encodes the table prefix
func (tke *TupleKeyEncoder) EncodeTablePrefix(prefix TupleKey,dbID uint64,tableID uint64) (TupleKey,*EncodedItem) {
	dbPre,_ := tke.EncodeDatabasePrefix(prefix,dbID)
	dbPrefix,_ := tke.oe.EncodeUint64(dbPre,tableID)
	return dbPrefix,nil
}

// EncodeIndexPrefix encodes the index prefix
func (tke *TupleKeyEncoder) EncodeIndexPrefix(prefix TupleKey,dbID uint64,tableID, indexID uint64) (TupleKey,*EncodedItem) {
	tablePre,_ := tke.EncodeTablePrefix(prefix,dbID,tableID)
	indexPrefix,_ := tke.oe.EncodeUint64(tablePre,indexID)
	return indexPrefix,nil
}