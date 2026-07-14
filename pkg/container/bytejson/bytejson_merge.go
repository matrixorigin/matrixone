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

package bytejson

import "bytes"

// MergePatch applies an RFC 7396 JSON merge patch to bj.
func (bj ByteJson) MergePatch(patch ByteJson) (ByteJson, error) {
	if patch.Type != TpCodeObject {
		return cloneByteJson(patch), nil
	}

	if bj.Type != TpCodeObject {
		emptyObject, err := buildJsonObject(nil, nil)
		if err != nil {
			return Null, err
		}
		bj = emptyObject
	}

	return mergePatchObjects(bj, patch)
}

// MergePreserve merges bj and other using MySQL JSON_MERGE_PRESERVE rules.
func (bj ByteJson) MergePreserve(other ByteJson) (ByteJson, error) {
	if bj.Type == TpCodeObject && other.Type == TpCodeObject {
		return mergePreserveObjects(bj, other)
	}

	capacity := 2
	if bj.Type == TpCodeArray {
		capacity += bj.GetElemCnt()
	}
	if other.Type == TpCodeArray {
		capacity += other.GetElemCnt()
	}
	elems := make([]ByteJson, 0, capacity)
	if bj.Type == TpCodeArray {
		for i := 0; i < bj.GetElemCnt(); i++ {
			elems = append(elems, bj.GetArrayElem(i))
		}
	} else {
		elems = append(elems, bj)
	}
	if other.Type == TpCodeArray {
		for i := 0; i < other.GetElemCnt(); i++ {
			elems = append(elems, other.GetArrayElem(i))
		}
	} else {
		elems = append(elems, other)
	}
	return buildBinaryJSONArray(elems), nil
}

func mergePatchObjects(target, patch ByteJson) (ByteJson, error) {
	keys := make([][]byte, 0, target.GetElemCnt()+patch.GetElemCnt())
	values := make([]ByteJson, 0, target.GetElemCnt()+patch.GetElemCnt())
	for targetIdx, patchIdx := 0, 0; targetIdx < target.GetElemCnt() || patchIdx < patch.GetElemCnt(); {
		switch {
		case patchIdx == patch.GetElemCnt():
			keys = append(keys, target.GetObjectKey(targetIdx))
			values = append(values, target.GetObjectVal(targetIdx))
			targetIdx++
		case targetIdx == target.GetElemCnt():
			patchValue := patch.GetObjectVal(patchIdx)
			if !isJSONNull(patchValue) {
				value, err := Null.MergePatch(patchValue)
				if err != nil {
					return Null, err
				}
				keys = append(keys, patch.GetObjectKey(patchIdx))
				values = append(values, value)
			}
			patchIdx++
		default:
			targetKey := target.GetObjectKey(targetIdx)
			patchKey := patch.GetObjectKey(patchIdx)
			cmp := bytes.Compare(targetKey, patchKey)
			switch {
			case cmp < 0:
				keys = append(keys, targetKey)
				values = append(values, target.GetObjectVal(targetIdx))
				targetIdx++
			case cmp > 0:
				patchValue := patch.GetObjectVal(patchIdx)
				if !isJSONNull(patchValue) {
					value, err := Null.MergePatch(patchValue)
					if err != nil {
						return Null, err
					}
					keys = append(keys, patchKey)
					values = append(values, value)
				}
				patchIdx++
			default:
				patchValue := patch.GetObjectVal(patchIdx)
				if !isJSONNull(patchValue) {
					value, err := target.GetObjectVal(targetIdx).MergePatch(patchValue)
					if err != nil {
						return Null, err
					}
					keys = append(keys, targetKey)
					values = append(values, value)
				}
				targetIdx++
				patchIdx++
			}
		}
	}
	return buildJsonObject(keys, values)
}

func mergePreserveObjects(left, right ByteJson) (ByteJson, error) {
	keys := make([][]byte, 0, left.GetElemCnt()+right.GetElemCnt())
	values := make([]ByteJson, 0, left.GetElemCnt()+right.GetElemCnt())
	for leftIdx, rightIdx := 0, 0; leftIdx < left.GetElemCnt() || rightIdx < right.GetElemCnt(); {
		switch {
		case rightIdx == right.GetElemCnt():
			keys = append(keys, left.GetObjectKey(leftIdx))
			values = append(values, left.GetObjectVal(leftIdx))
			leftIdx++
		case leftIdx == left.GetElemCnt():
			keys = append(keys, right.GetObjectKey(rightIdx))
			values = append(values, right.GetObjectVal(rightIdx))
			rightIdx++
		default:
			leftKey := left.GetObjectKey(leftIdx)
			rightKey := right.GetObjectKey(rightIdx)
			cmp := bytes.Compare(leftKey, rightKey)
			switch {
			case cmp < 0:
				keys = append(keys, leftKey)
				values = append(values, left.GetObjectVal(leftIdx))
				leftIdx++
			case cmp > 0:
				keys = append(keys, rightKey)
				values = append(values, right.GetObjectVal(rightIdx))
				rightIdx++
			default:
				value, err := left.GetObjectVal(leftIdx).MergePreserve(right.GetObjectVal(rightIdx))
				if err != nil {
					return Null, err
				}
				keys = append(keys, leftKey)
				values = append(values, value)
				leftIdx++
				rightIdx++
			}
		}
	}
	return buildJsonObject(keys, values)
}

func cloneByteJson(bj ByteJson) ByteJson {
	return ByteJson{Type: bj.Type, Data: append([]byte(nil), bj.Data...)}
}

func isJSONNull(bj ByteJson) bool {
	return bj.Type == TpCodeLiteral && len(bj.Data) == 1 && bj.Data[0] == LiteralNull
}
