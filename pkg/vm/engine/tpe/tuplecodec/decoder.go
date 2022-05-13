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
	"bytes"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorUnmatchedTenantPrefix = errors.New("unmatched prefix")
	errorUnmatchedValueType    = errors.New("unmatched value type 1")
)

func NewTupleKeyDecoder(tenantID uint64) *TupleKeyDecoder {
	od := orderedcodec.NewOrderedDecoder()
	tkd := &TupleKeyDecoder{od: od}
	return tkd
}

func (tkd *TupleKeyDecoder) GetTenantPrefix() TupleKey {
	return *tkd.tenantPrefix
}

// SkipTenantPrefix skips the tenant prefix wanted and returns the rest.
func (tkd *TupleKeyDecoder) SkipTenantPrefix(key TupleKey) (TupleKey, error) {
	tp := tkd.GetTenantPrefix()
	if !bytes.HasPrefix(key, tp) {
		return nil, errorUnmatchedTenantPrefix
	}

	return key[len(tp):], nil
}

// DecodeDatabasePrefix decodes database ID and returns the rest.
func (tkd *TupleKeyDecoder) DecodeDatabasePrefix(key TupleKey) (TupleKey, *orderedcodec.DecodedItem, error) {
	tk, err := tkd.SkipTenantPrefix(key)
	if err != nil {
		return nil, nil, err
	}
	rest, di, err := tkd.od.DecodeUint64(tk)
	if err != nil {
		return nil, nil, err
	}
	if !di.IsValueType(orderedcodec.VALUE_TYPE_UINT64) {
		return nil, nil, errorUnmatchedValueType
	}
	di.SetSectionType(orderedcodec.SECTION_TYPE_DATABASEID)
	return rest, di, err
}

// DecodeTablePrefix decodes table ID and returns the rest.
func (tkd *TupleKeyDecoder) DecodeTablePrefix(key TupleKey) (TupleKey, []*orderedcodec.DecodedItem, error) {
	rest, di, err := tkd.DecodeDatabasePrefix(key)
	if err != nil {
		return nil, nil, err
	}
	rest2, di2, err2 := tkd.od.DecodeUint64(rest)
	if err2 != nil {
		return nil, nil, err2
	}
	if !di2.IsValueType(orderedcodec.VALUE_TYPE_UINT64) {
		return nil, nil, errorUnmatchedValueType
	}
	di2.SetSectionType(orderedcodec.SECTION_TYPE_TABLEID)
	return rest2, []*orderedcodec.DecodedItem{di, di2}, err2
}

// DecodeIndexPrefix decodes database ID, table ID and index ID and returns the rest.
func (tkd *TupleKeyDecoder) DecodeIndexPrefix(key TupleKey) (TupleKey, []*orderedcodec.DecodedItem, error) {
	rest, di, err := tkd.DecodeTablePrefix(key)
	if err != nil {
		return nil, nil, err
	}
	rest2, di2, err2 := tkd.od.DecodeUint64(rest)
	if err2 != nil {
		return nil, nil, err2
	}
	if !di2.IsValueType(orderedcodec.VALUE_TYPE_UINT64) {
		return nil, nil, errorUnmatchedValueType
	}
	di2.SetSectionType(orderedcodec.SECTION_TYPE_INDEXID)
	di = append(di, di2)
	return rest2, di, err2
}

// DecodePrimaryIndexKey decodes fields of the primary index and returns the rest.
// The dbID,tableID and Index ID have been decoded.
func (tkd *TupleKeyDecoder) DecodePrimaryIndexKey(key TupleKey, index *descriptor.IndexDesc) (TupleKey, []*orderedcodec.DecodedItem, error) {
	if index.ID != PrimaryIndexID {
		return nil, nil, errorPrimaryIndexIDIsNotOne
	}

	retDis := make([]*orderedcodec.DecodedItem, 0, len(index.Attributes))
	//needs attribute type
	rest := key
	for _, attr := range index.Attributes {
		rest2, di, err := tkd.od.DecodeKey(rest, attr.Type)
		if err != nil {
			return nil, nil, err
		}
		if attr.Type == orderedcodec.VALUE_TYPE_STRING {
			di.Value = string(di.Value.([]byte))
			di.ValueType = attr.Type
		}
		retDis = append(retDis, di)
		rest = rest2
	}
	return rest, retDis, nil
}

// DecodePrimaryIndexValue decodes the values of the primary index and return the rest.
// Now,it decodes all tuple.
func (tkd *TupleKeyDecoder) DecodePrimaryIndexValue(value TupleValue, index *descriptor.IndexDesc, columnGroupID uint64, serializer ValueSerializer) (TupleValue, []*orderedcodec.DecodedItem, error) {
	rest := value
	var retDis []*orderedcodec.DecodedItem
	for len(rest) > 0 {
		rest2, di, err := serializer.DeserializeValue(rest)
		if err != nil {
			return nil, nil, err
		}
		retDis = append(retDis, di)
		rest = rest2
	}
	return rest, retDis, nil
}
