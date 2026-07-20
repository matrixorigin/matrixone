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

import (
	"bytes"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ByteJsonDataEncoder writes the Data portion of one finalized ByteJson value.
// The leading type byte is owned by the storage sink.
type ByteJsonDataEncoder interface {
	TypeCode() TpCode
	DataSize() uint32
	EncodeDataInto(dst []byte) (int, error)
}

type mergeMode uint8

const (
	mergeModeInvalid mergeMode = iota
	mergeModePatch
	mergeModePreserve
)

type mergeBuilderState uint8

const (
	mergeBuilderUnconfigured mergeBuilderState = iota
	mergeBuilderCleared
	mergeBuilderBuilding
	mergeBuilderFinalized
	mergeBuilderFailed
)

type mergeValueKind uint8

const (
	mergeValueInvalid mergeValueKind = iota
	mergeValueRaw
	mergeValueObject
	mergeValueArray
)

type mergeValue struct {
	kind     mergeValueKind
	raw      ByteJson
	object   *mergeObjectPlan
	array    *mergeArrayPlan
	dataSize uint32
	depth    uint16
}

type mergeObjectPlan struct {
	entries map[string]*mergeObjectEntry
	sorted  []*mergeObjectEntry
}

type mergeObjectEntry struct {
	key   string
	value mergeValue
}

type mergeArraySegment struct {
	rawArray ByteJson
	single   mergeValue
	isRaw    bool
}

type mergeArrayPlan struct {
	segments  []mergeArraySegment
	elemCount uint32
}

// MergeBuilder accumulates all JSON merge arguments for one row and encodes
// the final logical value once.
type MergeBuilder struct {
	mode        mergeMode
	state       mergeBuilderState
	root        mergeValue
	initialized bool
	rootValid   bool
}

func NewMergePatchBuilder() *MergeBuilder {
	return &MergeBuilder{mode: mergeModePatch, state: mergeBuilderCleared}
}

func NewMergePreserveBuilder() *MergeBuilder {
	return &MergeBuilder{mode: mergeModePreserve, state: mergeBuilderCleared}
}

func (b *MergeBuilder) BeginRow() error {
	if !b.configured() {
		return newMergeBuilderStateError("builder is not configured")
	}
	if b.state != mergeBuilderCleared {
		return newMergeBuilderStateError("previous row is not cleared")
	}
	b.root = mergeValue{}
	b.initialized = false
	b.rootValid = false
	b.state = mergeBuilderBuilding
	return nil
}

func (b *MergeBuilder) ResetUnknown() error {
	if err := b.requireBuilding(); err != nil {
		return err
	}
	b.root = mergeValue{}
	b.initialized = false
	b.rootValid = false
	return nil
}

func (b *MergeBuilder) Reset(document ByteJson) error {
	if err := b.requireBuilding(); err != nil {
		return err
	}
	if err := ValidateJSONMergeDocument(document); err != nil {
		return b.fail(err)
	}
	b.root = newRawMergeValue(document)
	b.initialized = true
	b.rootValid = true
	return nil
}

// ValidateJSONMergeDocument enforces the nesting-depth contract for one input
// document independently of how the merge state machine will consume it.
func ValidateJSONMergeDocument(document ByteJson) error {
	value := newRawMergeValue(document)
	return ensureMergeValueFits(&value, 1)
}

// Merge validates both participating roots before mutating the logical tree.
func (b *MergeBuilder) Merge(document ByteJson) error {
	if err := b.requireBuilding(); err != nil {
		return err
	}
	if !b.initialized {
		return b.fail(newMergeBuilderStateError("builder has no current value"))
	}
	if !b.rootValid {
		if err := ensureMergeValueFits(&b.root, 1); err != nil {
			return b.fail(err)
		}
		b.rootValid = true
	}
	incoming := newRawMergeValue(document)
	if err := ensureMergeValueFits(&incoming, 1); err != nil {
		return b.fail(err)
	}

	var err error
	switch b.mode {
	case mergeModePatch:
		err = mergePatchInto(&b.root, incoming, 1)
	case mergeModePreserve:
		err = mergePreserveInto(&b.root, incoming, 1)
	default:
		err = newMergeBuilderStateError("invalid merge mode")
	}
	if err != nil {
		return b.fail(err)
	}
	b.rootValid = true
	return nil
}

func (b *MergeBuilder) Finalize() error {
	if b.state == mergeBuilderFinalized {
		return nil
	}
	if err := b.requireBuilding(); err != nil {
		return err
	}
	if !b.initialized {
		return b.fail(newMergeBuilderStateError("builder has no current value"))
	}
	_, depth, err := finalizeMergeValue(&b.root)
	if err != nil {
		return b.fail(err)
	}
	if depth > maxJSONMergeNestingDepth {
		return b.fail(newJSONMergeDepthError())
	}
	b.state = mergeBuilderFinalized
	return nil
}

func (b *MergeBuilder) BuildOwned() (ByteJson, error) {
	if err := b.Finalize(); err != nil {
		return Null, err
	}
	buf := make([]byte, int(b.root.dataSize))
	n, err := b.EncodeDataInto(buf)
	if err != nil {
		return Null, err
	}
	if n != len(buf) {
		return Null, b.fail(newMergeSizeMismatchError(len(buf), n))
	}
	return ByteJson{Type: b.TypeCode(), Data: buf}, nil
}

func (b *MergeBuilder) TypeCode() TpCode {
	if b.state != mergeBuilderFinalized {
		return 0
	}
	return b.root.typeCode()
}

func (b *MergeBuilder) DataSize() uint32 {
	if b.state != mergeBuilderFinalized {
		return 0
	}
	return b.root.dataSize
}

func (b *MergeBuilder) EncodeDataInto(dst []byte) (int, error) {
	if b.state != mergeBuilderFinalized {
		return 0, newMergeBuilderStateError("builder is not finalized")
	}
	if uint64(len(dst)) != uint64(b.root.dataSize) {
		return 0, b.fail(newMergeSizeMismatchError(int(b.root.dataSize), len(dst)))
	}
	n, err := encodeMergeValueData(&b.root, dst)
	if err != nil {
		return 0, b.fail(err)
	}
	if n != len(dst) {
		return 0, b.fail(newMergeSizeMismatchError(len(dst), n))
	}
	return n, nil
}

func (b *MergeBuilder) Clear() {
	b.root = mergeValue{}
	b.initialized = false
	b.rootValid = false
	if b.configured() {
		b.state = mergeBuilderCleared
	} else {
		b.state = mergeBuilderUnconfigured
	}
}

func (b *MergeBuilder) configured() bool {
	return b.mode == mergeModePatch || b.mode == mergeModePreserve
}

func (b *MergeBuilder) requireBuilding() error {
	if !b.configured() {
		return newMergeBuilderStateError("builder is not configured")
	}
	if b.state != mergeBuilderBuilding {
		return newMergeBuilderStateError("builder is not building")
	}
	return nil
}

func (b *MergeBuilder) fail(err error) error {
	b.state = mergeBuilderFailed
	return err
}

func newRawMergeValue(value ByteJson) mergeValue {
	return mergeValue{kind: mergeValueRaw, raw: value}
}

func (v *mergeValue) typeCode() TpCode {
	switch v.kind {
	case mergeValueRaw:
		return v.raw.Type
	case mergeValueObject:
		return TpCodeObject
	case mergeValueArray:
		return TpCodeArray
	default:
		return 0
	}
}

func (v *mergeValue) literalCode() byte {
	if v.kind == mergeValueRaw && v.raw.Type == TpCodeLiteral && len(v.raw.Data) > 0 {
		return v.raw.Data[0]
	}
	return 0
}

func (v *mergeValue) isJSONNull() bool {
	return v.typeCode() == TpCodeLiteral && v.literalCode() == LiteralNull
}

func ensureMergeValueFits(value *mergeValue, outputDepth int) error {
	depth, err := mergeValueDepth(value)
	if err != nil {
		return err
	}
	if depth == 0 {
		return nil
	}
	if outputDepth+depth-1 > maxJSONMergeNestingDepth {
		return newJSONMergeDepthError()
	}
	return nil
}

func mergeValueDepth(value *mergeValue) (int, error) {
	switch value.kind {
	case mergeValueRaw:
		return byteJsonNestingDepth(value.raw), nil
	case mergeValueObject:
		depth := 1
		for _, entry := range value.object.entries {
			childDepth, err := mergeValueDepth(&entry.value)
			if err != nil {
				return 0, err
			}
			if childDepth > 0 && childDepth+1 > depth {
				depth = childDepth + 1
			}
		}
		return depth, nil
	case mergeValueArray:
		depth := 1
		err := value.array.forEach(func(child *mergeValue) error {
			childDepth, err := mergeValueDepth(child)
			if err != nil {
				return err
			}
			if childDepth > 0 && childDepth+1 > depth {
				depth = childDepth + 1
			}
			return nil
		})
		return depth, err
	default:
		return 0, newMergeBuilderStateError("invalid merge value")
	}
}

func byteJsonNestingDepth(value ByteJson) int {
	if value.Type != TpCodeObject && value.Type != TpCodeArray {
		return 0
	}
	maxDepth := 1
	type frame struct {
		value ByteJson
		next  int
		depth int
	}
	var frames [maxJSONMergeNestingDepth]frame
	frames[0] = frame{value: value, depth: 1}
	stack := frames[:1]
	for len(stack) > 0 {
		current := &stack[len(stack)-1]
		if current.next == current.value.GetElemCnt() {
			stack = stack[:len(stack)-1]
			continue
		}
		var child ByteJson
		if current.value.Type == TpCodeObject {
			child = current.value.GetObjectVal(current.next)
		} else {
			child = current.value.GetArrayElem(current.next)
		}
		current.next++
		if child.Type != TpCodeObject && child.Type != TpCodeArray {
			continue
		}
		depth := current.depth + 1
		if depth > maxDepth {
			maxDepth = depth
		}
		if maxDepth > maxJSONMergeNestingDepth {
			return maxDepth
		}
		stack = frames[:len(stack)+1]
		stack[len(stack)-1] = frame{value: child, depth: depth}
	}
	return maxDepth
}

func mergePatchInto(target *mergeValue, patch mergeValue, outputDepth int) error {
	if patch.typeCode() != TpCodeObject {
		if err := ensureMergeValueFits(&patch, outputDepth); err != nil {
			return err
		}
		*target = patch
		return nil
	}
	if outputDepth > maxJSONMergeNestingDepth {
		return newJSONMergeDepthError()
	}
	if target.typeCode() != TpCodeObject {
		*target = mergeValue{
			kind: mergeValueObject,
			object: &mergeObjectPlan{
				entries: make(map[string]*mergeObjectEntry),
			},
		}
	} else if err := promoteMergeObject(target); err != nil {
		return err
	}

	for i := 0; i < patch.raw.GetElemCnt(); i++ {
		keyBytes := patch.raw.GetObjectKey(i)
		patchValue := newRawMergeValue(patch.raw.GetObjectVal(i))
		key := string(keyBytes)
		if patchValue.isJSONNull() {
			delete(target.object.entries, key)
			target.object.sorted = nil
			continue
		}
		entry, ok := target.object.entries[key]
		if !ok {
			child := newRawMergeValue(Null)
			if err := mergePatchInto(&child, patchValue, outputDepth+1); err != nil {
				return err
			}
			ownedKey := string(bytes.Clone(keyBytes))
			target.object.entries[ownedKey] = &mergeObjectEntry{key: ownedKey, value: child}
			target.object.sorted = nil
			continue
		}
		if err := mergePatchInto(&entry.value, patchValue, outputDepth+1); err != nil {
			return err
		}
		target.object.sorted = nil
	}
	return nil
}

func mergePreserveInto(left *mergeValue, right mergeValue, outputDepth int) error {
	if left.typeCode() == TpCodeObject && right.typeCode() == TpCodeObject {
		if outputDepth > maxJSONMergeNestingDepth {
			return newJSONMergeDepthError()
		}
		if err := promoteMergeObject(left); err != nil {
			return err
		}
		for i := 0; i < right.raw.GetElemCnt(); i++ {
			keyBytes := right.raw.GetObjectKey(i)
			rightValue := newRawMergeValue(right.raw.GetObjectVal(i))
			entry, ok := left.object.entries[string(keyBytes)]
			if !ok {
				if err := ensureMergeValueFits(&rightValue, outputDepth+1); err != nil {
					return err
				}
				ownedKey := string(bytes.Clone(keyBytes))
				left.object.entries[ownedKey] = &mergeObjectEntry{key: ownedKey, value: rightValue}
				left.object.sorted = nil
				continue
			}
			if err := mergePreserveInto(&entry.value, rightValue, outputDepth+1); err != nil {
				return err
			}
			left.object.sorted = nil
		}
		return nil
	}
	return mergePreserveArrayInto(left, right, outputDepth)
}

func promoteMergeObject(value *mergeValue) error {
	if value.kind == mergeValueObject {
		return nil
	}
	if value.kind != mergeValueRaw || value.raw.Type != TpCodeObject {
		return newMergeBuilderStateError("cannot promote non-object merge value")
	}
	plan := &mergeObjectPlan{entries: make(map[string]*mergeObjectEntry, value.raw.GetElemCnt())}
	for i := 0; i < value.raw.GetElemCnt(); i++ {
		key := string(bytes.Clone(value.raw.GetObjectKey(i)))
		plan.entries[key] = &mergeObjectEntry{
			key:   key,
			value: newRawMergeValue(value.raw.GetObjectVal(i)),
		}
	}
	*value = mergeValue{kind: mergeValueObject, object: plan}
	return nil
}

func mergePreserveArrayInto(left *mergeValue, right mergeValue, outputDepth int) error {
	if outputDepth > maxJSONMergeNestingDepth {
		return newJSONMergeDepthError()
	}
	if left.kind != mergeValueArray {
		oldLeft := *left
		*left = mergeValue{kind: mergeValueArray, array: &mergeArrayPlan{}}
		if err := left.array.appendValue(oldLeft, outputDepth); err != nil {
			return err
		}
	}
	return left.array.appendValue(right, outputDepth)
}

func (array *mergeArrayPlan) appendValue(value mergeValue, outputDepth int) error {
	if value.kind == mergeValueRaw && value.raw.Type == TpCodeArray {
		if err := ensureMergeValueFits(&value, outputDepth); err != nil {
			return err
		}
		count := uint64(array.elemCount) + uint64(value.raw.GetElemCnt())
		if count > math.MaxUint32 {
			return newMergeSizeOverflowError()
		}
		array.elemCount = uint32(count)
		array.segments = append(array.segments, mergeArraySegment{rawArray: value.raw, isRaw: true})
		return nil
	}
	if err := ensureMergeValueFits(&value, outputDepth+1); err != nil {
		return err
	}
	if array.elemCount == math.MaxUint32 {
		return newMergeSizeOverflowError()
	}
	array.elemCount++
	array.segments = append(array.segments, mergeArraySegment{single: value})
	return nil
}

func (array *mergeArrayPlan) forEach(fn func(value *mergeValue) error) error {
	for i := range array.segments {
		segment := &array.segments[i]
		if !segment.isRaw {
			if err := fn(&segment.single); err != nil {
				return err
			}
			continue
		}
		for j := 0; j < segment.rawArray.GetElemCnt(); j++ {
			value := newRawMergeValue(segment.rawArray.GetArrayElem(j))
			if err := fn(&value); err != nil {
				return err
			}
		}
	}
	return nil
}

func finalizeMergeValue(value *mergeValue) (uint32, int, error) {
	switch value.kind {
	case mergeValueRaw:
		if uint64(len(value.raw.Data)) > math.MaxUint32 {
			return 0, 0, newMergeSizeOverflowError()
		}
		value.dataSize = uint32(len(value.raw.Data))
		depth := byteJsonNestingDepth(value.raw)
		value.depth = uint16(depth)
		return value.dataSize, depth, nil
	case mergeValueObject:
		value.object.sorted = value.object.sorted[:0]
		for _, entry := range value.object.entries {
			value.object.sorted = append(value.object.sorted, entry)
		}
		slices.SortFunc(value.object.sorted, func(a, b *mergeObjectEntry) int {
			return bytes.Compare([]byte(a.key), []byte(b.key))
		})
		if uint64(len(value.object.sorted)) > math.MaxUint32 {
			return 0, 0, newMergeSizeOverflowError()
		}
		total := uint64(headerSize) + uint64(len(value.object.sorted))*uint64(keyEntrySize+valEntrySize)
		depth := 1
		for _, entry := range value.object.sorted {
			if len(entry.key) > math.MaxUint16 {
				return 0, 0, newMergeSizeOverflowError()
			}
			total += uint64(len(entry.key))
			childSize, childDepth, err := finalizeMergeValue(&entry.value)
			if err != nil {
				return 0, 0, err
			}
			if entry.value.typeCode() != TpCodeLiteral {
				total += uint64(childSize)
			}
			if childDepth > 0 && childDepth+1 > depth {
				depth = childDepth + 1
			}
			if total > math.MaxUint32 {
				return 0, 0, newMergeSizeOverflowError()
			}
		}
		value.dataSize = uint32(total)
		value.depth = uint16(depth)
		return value.dataSize, depth, nil
	case mergeValueArray:
		total := uint64(headerSize) + uint64(value.array.elemCount)*uint64(valEntrySize)
		if total > math.MaxUint32 {
			return 0, 0, newMergeSizeOverflowError()
		}
		depth := 1
		err := value.array.forEach(func(child *mergeValue) error {
			childSize, childDepth, err := finalizeMergeValue(child)
			if err != nil {
				return err
			}
			if child.typeCode() != TpCodeLiteral {
				total += uint64(childSize)
			}
			if childDepth > 0 && childDepth+1 > depth {
				depth = childDepth + 1
			}
			if total > math.MaxUint32 {
				return newMergeSizeOverflowError()
			}
			return nil
		})
		if err != nil {
			return 0, 0, err
		}
		value.dataSize = uint32(total)
		value.depth = uint16(depth)
		return value.dataSize, depth, nil
	default:
		return 0, 0, newMergeBuilderStateError("invalid merge value")
	}
}

func encodeMergeValueData(value *mergeValue, dst []byte) (int, error) {
	switch value.kind {
	case mergeValueRaw:
		return copy(dst, value.raw.Data), nil
	case mergeValueObject:
		return encodeMergeObject(value.object, dst)
	case mergeValueArray:
		return encodeMergeArray(value.array, dst)
	default:
		return 0, newMergeBuilderStateError("invalid merge value")
	}
}

func encodeMergeObject(object *mergeObjectPlan, dst []byte) (int, error) {
	if len(dst) < headerSize {
		return 0, newMergeSizeMismatchError(headerSize, len(dst))
	}
	endian.PutUint32(dst, uint32(len(object.sorted)))
	endian.PutUint32(dst[docSizeOff:], uint32(len(dst)))
	valueEntryStart := headerSize + len(object.sorted)*keyEntrySize
	payloadOffset := headerSize + len(object.sorted)*(keyEntrySize+valEntrySize)
	for i, entry := range object.sorted {
		keyEntry := dst[headerSize+i*keyEntrySize:]
		endian.PutUint32(keyEntry, uint32(payloadOffset))
		endian.PutUint16(keyEntry[keyOriginOff:], uint16(len(entry.key)))
		copy(dst[payloadOffset:], entry.key)
		payloadOffset += len(entry.key)
	}
	for i, entry := range object.sorted {
		valueEntry := dst[valueEntryStart+i*valEntrySize:]
		valueEntry[0] = entry.value.typeCode()
		if entry.value.typeCode() == TpCodeLiteral {
			endian.PutUint32(valueEntry[valTypeSize:], uint32(entry.value.literalCode()))
			continue
		}
		endian.PutUint32(valueEntry[valTypeSize:], uint32(payloadOffset))
		size := int(entry.value.dataSize)
		n, err := encodeMergeValueData(&entry.value, dst[payloadOffset:payloadOffset+size])
		if err != nil {
			return 0, err
		}
		if n != size {
			return 0, newMergeSizeMismatchError(size, n)
		}
		payloadOffset += size
	}
	if payloadOffset != len(dst) {
		return 0, newMergeSizeMismatchError(len(dst), payloadOffset)
	}
	return payloadOffset, nil
}

func encodeMergeArray(array *mergeArrayPlan, dst []byte) (int, error) {
	if len(dst) < headerSize {
		return 0, newMergeSizeMismatchError(headerSize, len(dst))
	}
	endian.PutUint32(dst, array.elemCount)
	endian.PutUint32(dst[docSizeOff:], uint32(len(dst)))
	payloadOffset := headerSize + int(array.elemCount)*valEntrySize
	entryIndex := 0
	err := array.forEach(func(value *mergeValue) error {
		valueEntry := dst[headerSize+entryIndex*valEntrySize:]
		entryIndex++
		valueEntry[0] = value.typeCode()
		if value.typeCode() == TpCodeLiteral {
			endian.PutUint32(valueEntry[valTypeSize:], uint32(value.literalCode()))
			return nil
		}
		endian.PutUint32(valueEntry[valTypeSize:], uint32(payloadOffset))
		size := int(value.dataSize)
		if value.kind == mergeValueRaw && value.dataSize == 0 {
			size = len(value.raw.Data)
		}
		n, err := encodeMergeValueData(value, dst[payloadOffset:payloadOffset+size])
		if err != nil {
			return err
		}
		if n != size {
			return newMergeSizeMismatchError(size, n)
		}
		payloadOffset += size
		return nil
	})
	if err != nil {
		return 0, err
	}
	if entryIndex != int(array.elemCount) || payloadOffset != len(dst) {
		return 0, newMergeSizeMismatchError(len(dst), payloadOffset)
	}
	return payloadOffset, nil
}

func newJSONMergeDepthError() error {
	return moerr.NewInvalidInputNoCtxf(
		"json document nesting depth exceeds %d", maxJSONMergeNestingDepth,
	)
}

func newMergeSizeOverflowError() error {
	return moerr.NewInvalidInputNoCtx("json merge result is too large")
}

func newMergeSizeMismatchError(expected, actual int) error {
	return moerr.NewInternalErrorNoCtxf(
		"json merge encoded size mismatch: expected %d, got %d", expected, actual,
	)
}

func newMergeBuilderStateError(message string) error {
	return moerr.NewInternalErrorNoCtxf("json merge builder: %s", message)
}
