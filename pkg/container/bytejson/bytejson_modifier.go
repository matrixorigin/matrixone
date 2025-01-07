// Copyright 2024 Matrix Origin
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

package bytejson

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type bytejsonModifier struct {
	bj        ByteJson
	modifyPtr *byte
	modifyVal ByteJson
}

func (bm *bytejsonModifier) insert(path *Path, newBj ByteJson) (ByteJson, error) {
	result := bm.bj.querySimple(path)
	if CompareByteJson(result, Null) > 0 {
		// if path exists, return
		return bm.bj, nil
	}
	// insert
	if err := bm.doInsert(path, newBj); err == nil {
		return bm.rebuild(), nil
	}

	return Null, moerr.NewInvalidArgNoCtx("invalid json insert", path.String())
}

func (bm *bytejsonModifier) replace(path *Path, newBj ByteJson) (ByteJson, error) {
	result := bm.bj.querySimple(path)
	if CompareByteJson(result, Null) == 0 {
		// if path not exists, return
		return bm.bj, nil
	}
	// replace
	bm.modifyPtr = &result.Data[0]
	bm.modifyVal = newBj
	return bm.rebuild(), nil
}

func (bm *bytejsonModifier) set(path *Path, newBj ByteJson) (ByteJson, error) {
	result := bm.bj.querySimple(path)
	if CompareByteJson(result, Null) > 0 {
		// set
		bm.modifyPtr = &result.Data[0]
		bm.modifyVal = newBj
		return bm.rebuild(), nil
	}
	// insert
	if err := bm.doInsert(path, newBj); err == nil {
		return bm.rebuild(), nil
	}
	// return bm.rebuild()
	return Null, moerr.NewInvalidArgNoCtx("invalid path", path.String())
}

func (bm *bytejsonModifier) rebuild() ByteJson {
	buf := make([]byte, 0, len(bm.bj.Data)+len(bm.modifyVal.Data))
	value, tpCode := bm.rebuildTo(buf)
	return ByteJson{Type: tpCode, Data: value}
}

func (bm *bytejsonModifier) rebuildTo(buf []byte) ([]byte, TpCode) {
	if bm.modifyPtr == &bm.bj.Data[0] {
		bm.modifyPtr = nil
		return append(buf, bm.modifyVal.Data...), bm.modifyVal.Type
	} else if bm.modifyPtr == nil {
		return append(buf, bm.bj.Data...), bm.bj.Type
	}

	bj := bm.bj
	if bj.Type != TpCodeArray && bj.Type != TpCodeObject {
		return append(buf, bj.Data...), bj.Type
	}

	docOff := len(buf)
	elemCount := bj.GetElemCnt()

	var valEntryStart int
	if bj.Type == TpCodeArray {
		// json array
		copySize := headerSize + elemCount*valEntrySize
		valEntryStart = headerSize
		buf = append(buf, bj.Data[:copySize]...)
	} else {
		// josn object
		copySize := headerSize + elemCount*(keyEntrySize+valEntrySize)
		valEntryStart = headerSize + elemCount*keyEntrySize
		buf = append(buf, bj.Data[:copySize]...)
		if elemCount > 0 {
			firstKeyOff := int(endian.Uint32(bj.Data[headerSize:]))
			lastKeyOff := int(endian.Uint32(bj.Data[headerSize+keyEntrySize*(elemCount-1):]))
			lastKeyLen := int(endian.Uint16(bj.Data[headerSize+keyEntrySize*(elemCount-1)+docSizeOff:]))
			buf = append(buf, bj.Data[firstKeyOff:lastKeyOff+lastKeyLen]...)
		}
	}

	for i := 0; i < elemCount; i++ {
		valEntryOff := valEntryStart + i*valEntrySize
		elem := bj.getValEntry(valEntryOff)
		bm.bj = elem
		var tpCode TpCode
		valOff := len(buf) - docOff
		buf, tpCode = bm.rebuildTo(buf)
		buf[docOff+valEntryOff] = tpCode
		if tpCode == TpCodeLiteral {
			lastIdx := len(buf) - 1
			endian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(buf[lastIdx]))
			buf = buf[:lastIdx]
		} else {
			endian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(valOff))
		}
	}
	endian.PutUint32(buf[docOff+docSizeOff:], uint32(len(buf)-docOff))
	return buf, bj.Type
}

func (bm *bytejsonModifier) doInsert(path *Path, newBj ByteJson) (err error) {
	parentPath, lastSub := path.popOneSubPath()
	result := bm.bj.querySimple(&parentPath)
	if CompareByteJson(result, Null) == 0 {
		return
	}

	parent := result

	if lastSub.tp == subPathIdx {
		bm.modifyPtr = &parent.Data[0]
		if parent.Type != TpCodeArray {
			bm.modifyVal = buildBinaryJSONArray([]ByteJson{parent, newBj})
			return
		}
		elemCnt := parent.GetElemCnt()
		elems := make([]ByteJson, 0, elemCnt+1)
		for i := 0; i < elemCnt; i++ {
			elems = append(elems, parent.getArrayElem(i))
		}
		elems = append(elems, newBj)
		bm.modifyVal = buildBinaryJSONArray(elems)
		return
	}

	if parent.Type != TpCodeObject {
		return
	}

	bm.modifyPtr = &parent.Data[0]
	elementCount := parent.GetElemCnt()
	insertKey := lastSub.key
	inserIndx := sort.Search(elementCount, func(i int) bool {
		k := parent.getObjectKey(i)
		return bytes.Compare(k, []byte(insertKey)) >= 0
	})
	keys := make([][]byte, 0, elementCount+1)
	elems := make([]ByteJson, 0, elementCount+1)
	for i := 0; i < elementCount; i++ {
		if i == inserIndx {
			keys = append(keys, []byte(insertKey))
			elems = append(elems, newBj)
		}
		keys = append(keys, parent.getObjectKey(i))
		elems = append(elems, parent.getObjectVal(i))
	}
	if inserIndx == elementCount {
		keys = append(keys, []byte(insertKey))
		elems = append(elems, newBj)
	}
	bm.modifyVal, err = buildJsonObject(keys, elems)
	return
}
