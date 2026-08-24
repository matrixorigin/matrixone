// Copyright 2021 - 2022 Matrix Origin
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

package aggexec

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type bmp struct {
	legacy     *roaring.Bitmap
	values     []uint32
	mp         *mpool.MPool
	allocation *AllocationAccount
}

func (b *bmp) MarshalBinary() ([]byte, error) {
	var buffer []byte
	writer := &appendSliceWriter{data: buffer}
	if err := b.MarshalTo(writer); err != nil {
		return nil, err
	}
	return writer.data, nil
}
func (b *bmp) UnmarshalBinary(data []byte) error {
	return b.UnmarshalFromReader(bytes.NewReader(data))
}

func (b *bmp) UnmarshalFromReader(r io.Reader) error {
	if b == nil || r == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if b.allocation == nil {
		if b.legacy == nil {
			b.legacy = roaring.New()
		}
		_, err := b.legacy.ReadFrom(r)
		return err
	}
	candidate, err := decodeAccountedBitmap(r, b.mp, b.allocation)
	if err != nil {
		return err
	}
	old := b.values
	b.values = candidate
	if cap(old) > 0 {
		mpool.FreeSlice(b.mp, old)
	}
	return nil
}

func makeBmp(mp *mpool.MPool, allocation *AllocationAccount) (*bmp, error) {
	if mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if allocation == nil {
		return &bmp{legacy: roaring.New(), mp: mp}, nil
	}
	return &bmp{mp: mp, allocation: allocation}, nil
}

func makeBmpMarshalerUnmarshaler(mp *mpool.MPool, allocation *AllocationAccount) (MarshalerUnmarshaler, error) {
	return makeBmp(mp, allocation)
}

type bmpExecCommon struct {
	aggExec
}

func (b *bmp) add(value uint32) error {
	if b.allocation == nil {
		b.legacy.Add(value)
		return nil
	}
	index, found := slices.BinarySearch(b.values, value)
	if found {
		return nil
	}
	if len(b.values) == cap(b.values) {
		next, err := makeAccountedScratch[uint32](
			b.allocation, b.mp, max(1, cap(b.values)*2))
		if err != nil {
			return err
		}
		copy(next, b.values)
		if cap(b.values) > 0 {
			mpool.FreeSlice(b.mp, b.values)
		}
		b.values = next[:len(b.values)]
	}
	b.values = b.values[:len(b.values)+1]
	copy(b.values[index+1:], b.values[index:])
	b.values[index] = value
	return nil
}

func (b *bmp) normalize() {
	if b == nil || len(b.values) < 2 {
		return
	}
	slices.Sort(b.values)
	write := 1
	for _, value := range b.values[1:] {
		if value == b.values[write-1] {
			continue
		}
		b.values[write] = value
		write++
	}
	b.values = b.values[:write]
}

func (b *bmp) ensureCapacity(required int) error {
	if b == nil || required < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if b.allocation == nil || required <= cap(b.values) {
		return nil
	}
	capacity := max(1, cap(b.values))
	for capacity < required {
		if capacity > math.MaxInt/2 {
			capacity = required
			break
		}
		capacity *= 2
	}
	next, err := makeAccountedScratch[uint32](
		b.allocation, b.mp, capacity)
	if err != nil {
		return err
	}
	copy(next, b.values)
	if cap(b.values) > 0 {
		mpool.FreeSlice(b.mp, b.values)
	}
	b.values = next[:len(b.values)]
	return nil
}

func (b *bmp) unionLegacy(other *roaring.Bitmap) error {
	if b.allocation == nil {
		b.legacy.Or(other)
		return nil
	}
	cardinality := other.GetCardinality()
	if cardinality > uint64(math.MaxInt-len(b.values)) {
		return mpool.ErrAllocationAllocatorLimit
	}
	for iterator := other.Iterator(); iterator.HasNext(); {
		if err := b.add(iterator.Next()); err != nil {
			return err
		}
	}
	return nil
}

func (b *bmp) union(other *bmp) error {
	if b == nil || other == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if b.allocation == nil {
		if other.allocation != nil {
			for _, value := range other.values {
				b.legacy.Add(value)
			}
			return nil
		}
		b.legacy.Or(other.legacy)
		return nil
	}
	if other.allocation == nil {
		return b.unionLegacy(other.legacy)
	}
	return b.mergeSorted(other.values)
}

func (b *bmp) appendSorted(values []uint32) error {
	if b == nil || b.allocation == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if len(values) == 0 {
		return nil
	}
	if err := b.ensureCapacity(len(b.values) + len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if len(b.values) != 0 && value <= b.values[len(b.values)-1] {
			return mpool.ErrAllocationAccountInvariant
		}
		b.values = append(b.values, value)
	}
	return nil
}

func (b *bmp) mergeSorted(values []uint32) error {
	if b == nil || b.allocation == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if len(values) == 0 {
		return nil
	}
	if len(b.values) == 0 {
		return b.appendSorted(values)
	}
	oldLength := len(b.values)
	if err := b.ensureCapacity(oldLength + len(values)); err != nil {
		return err
	}
	b.values = b.values[:oldLength+len(values)]
	write, right := len(b.values)-1, len(values)-1
	for left := oldLength - 1; left >= 0; left-- {
		for right >= 0 && values[right] > b.values[left] {
			b.values[write] = values[right]
			write--
			right--
		}
		if right >= 0 && values[right] == b.values[left] {
			right--
		}
		b.values[write] = b.values[left]
		write--
	}
	for right >= 0 {
		b.values[write] = values[right]
		write--
		right--
	}
	start := write + 1
	copy(b.values, b.values[start:])
	b.values = b.values[:len(b.values)-start]
	return nil
}

const (
	bitmapSerialCookieNoRun = uint32(12346)
	bitmapSerialCookie      = uint32(12347)
	bitmapNoOffsetThreshold = 4
)

type bitmapContainerDescriptor struct {
	key         uint16
	cardinality uint32
	run         bool
}

func readBitmapWord(r io.Reader, size int) ([8]byte, error) {
	var word [8]byte
	if _, err := io.ReadFull(r, word[:size]); err != nil {
		return word, err
	}
	return word, nil
}

// scanAccountedBitmapWire validates one portable Roaring bitmap without
// constructing library containers. If output is non-nil, it also decodes the
// sorted values into the caller-provided storage. The wire carries all
// descriptors in its header, so direct indexing keeps both admission and
// publication allocation-free regardless of the number of containers.
func scanAccountedBitmapWire(data []byte, output []uint32) (int, error) {
	if len(data) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	cookie := binary.LittleEndian.Uint32(data[:4])
	var (
		count       int
		runMapStart = -1
		headerStart int
	)
	switch {
	case cookie == bitmapSerialCookieNoRun:
		if len(data) < 8 {
			return 0, io.ErrUnexpectedEOF
		}
		rawCount := binary.LittleEndian.Uint32(data[4:8])
		if rawCount > 1<<16 {
			return 0, moerr.NewInternalErrorNoCtxf(
				"invalid roaring bitmap container count %d", rawCount)
		}
		count = int(rawCount)
		headerStart = 8
	case cookie&math.MaxUint16 == bitmapSerialCookie:
		count = int(cookie>>16) + 1
		runMapStart = 4
		headerStart = runMapStart + (count+7)/8
	default:
		return 0, moerr.NewInternalErrorNoCtxf("invalid roaring bitmap cookie %d", cookie)
	}
	descriptorBytes := count * 4
	if headerStart > len(data) || descriptorBytes > len(data)-headerStart {
		return 0, io.ErrUnexpectedEOF
	}
	offsetStart := headerStart + descriptorBytes
	hasOffsets := runMapStart < 0 || count >= bitmapNoOffsetThreshold
	containerStart := offsetStart
	if hasOffsets {
		offsetBytes := count * 4
		if offsetBytes > len(data)-containerStart {
			return 0, io.ErrUnexpectedEOF
		}
		containerStart += offsetBytes
	}

	total := uint64(0)
	previousKey := -1
	for i := 0; i < count; i++ {
		descriptor := data[headerStart+i*4 : headerStart+(i+1)*4]
		key := int(binary.LittleEndian.Uint16(descriptor[:2]))
		if key <= previousKey {
			return 0, moerr.NewInternalErrorNoCtx("roaring bitmap container keys are not increasing")
		}
		previousKey = key
		total += uint64(binary.LittleEndian.Uint16(descriptor[2:4])) + 1
		if total > uint64(math.MaxInt) {
			return 0, mpool.ErrAllocationAllocatorLimit
		}
	}
	if output != nil && len(output) < int(total) {
		return 0, mpool.ErrAllocationAccountInvariant
	}

	position := containerStart
	written := 0
	for i := 0; i < count; i++ {
		if hasOffsets {
			offset := binary.LittleEndian.Uint32(
				data[offsetStart+i*4 : offsetStart+(i+1)*4])
			if uint64(position) != uint64(offset) {
				return 0, moerr.NewInternalErrorNoCtx("invalid roaring bitmap container offset")
			}
		}
		descriptor := data[headerStart+i*4 : headerStart+(i+1)*4]
		key := binary.LittleEndian.Uint16(descriptor[:2])
		cardinality := int(binary.LittleEndian.Uint16(descriptor[2:4])) + 1
		prefix := uint32(key) << 16
		isRun := runMapStart >= 0 &&
			data[runMapStart+i/8]&(1<<uint(i%8)) != 0
		switch {
		case isRun:
			if len(data)-position < 2 {
				return 0, io.ErrUnexpectedEOF
			}
			runs := int(binary.LittleEndian.Uint16(data[position : position+2]))
			position += 2
			if runs > (len(data)-position)/4 {
				return 0, io.ErrUnexpectedEOF
			}
			produced := 0
			previousEnd := -1
			for run := 0; run < runs; run++ {
				entry := data[position : position+4]
				position += 4
				start := int(binary.LittleEndian.Uint16(entry[:2]))
				length := int(binary.LittleEndian.Uint16(entry[2:4])) + 1
				if start <= previousEnd || length > 1<<16-start ||
					produced > cardinality-length {
					return 0, moerr.NewInternalErrorNoCtx("invalid roaring run container")
				}
				if output != nil {
					for value := start; value < start+length; value++ {
						output[written] = prefix | uint32(value)
						written++
					}
				}
				produced += length
				previousEnd = start + length - 1
			}
			if produced != cardinality {
				return 0, moerr.NewInternalErrorNoCtx("roaring run cardinality mismatch")
			}
			if output == nil {
				written += produced
			}
		case cardinality <= 4096:
			bytesNeeded := cardinality * 2
			if bytesNeeded > len(data)-position {
				return 0, io.ErrUnexpectedEOF
			}
			previousValue := -1
			for valueIndex := 0; valueIndex < cardinality; valueIndex++ {
				value := int(binary.LittleEndian.Uint16(
					data[position : position+2]))
				position += 2
				if value <= previousValue {
					return 0, moerr.NewInternalErrorNoCtx("roaring array values are not increasing")
				}
				if output != nil {
					output[written] = prefix | uint32(value)
				}
				written++
				previousValue = value
			}
		default:
			const bitmapBytes = 1024 * 8
			if bitmapBytes > len(data)-position {
				return 0, io.ErrUnexpectedEOF
			}
			produced := 0
			for bitmapWord := 0; bitmapWord < 1024; bitmapWord++ {
				valueBits := binary.LittleEndian.Uint64(
					data[position : position+8])
				position += 8
				produced += bits.OnesCount64(valueBits)
				if output != nil {
					for valueBits != 0 {
						bit := bits.TrailingZeros64(valueBits)
						output[written] = prefix | uint32(bitmapWord*64+bit)
						written++
						valueBits &= valueBits - 1
					}
				}
			}
			if produced != cardinality {
				return 0, moerr.NewInternalErrorNoCtx("roaring bitmap cardinality mismatch")
			}
			if output == nil {
				written += produced
			}
		}
	}
	if position != len(data) || written != int(total) {
		return 0, moerr.NewInternalErrorNoCtx("roaring bitmap decoded cardinality mismatch")
	}
	return int(total), nil
}

// decodeAccountedBitmap streams the portable Roaring representation into one
// account-owned sorted value slice. Descriptors are bounded by the format's
// 2^16-container limit and are themselves allocated through the same account.
func decodeAccountedBitmap(
	r io.Reader,
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (_ []uint32, retErr error) {
	if r == nil || mp == nil || allocation == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	word, err := readBitmapWord(r, 4)
	if err != nil {
		return nil, err
	}
	cookie := binary.LittleEndian.Uint32(word[:4])
	var (
		count  uint32
		runMap []byte
	)
	switch {
	case cookie == bitmapSerialCookieNoRun:
		word, err = readBitmapWord(r, 4)
		if err != nil {
			return nil, err
		}
		count = binary.LittleEndian.Uint32(word[:4])
	case cookie&math.MaxUint16 == bitmapSerialCookie:
		count = cookie>>16 + 1
		runBytes := (int(count) + 7) / 8
		if runBytes != 0 {
			runMap, err = allocation.allocArgumentArena(mp, runBytes)
			if err != nil {
				return nil, err
			}
			defer mp.Free(runMap)
			if _, err = io.ReadFull(r, runMap); err != nil {
				return nil, err
			}
		}
	default:
		return nil, moerr.NewInternalErrorNoCtxf("invalid roaring bitmap cookie %d", cookie)
	}
	if count > 1<<16 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid roaring bitmap container count %d", count)
	}
	descriptors, err := makeAccountedScratch[bitmapContainerDescriptor](
		allocation, mp, int(count))
	if err != nil {
		return nil, err
	}
	if cap(descriptors) > 0 {
		defer mpool.FreeSlice(mp, descriptors)
	}
	var total uint64
	var previous uint16
	for i := range descriptors {
		word, err = readBitmapWord(r, 4)
		if err != nil {
			return nil, err
		}
		descriptors[i] = bitmapContainerDescriptor{
			key:         binary.LittleEndian.Uint16(word[:2]),
			cardinality: uint32(binary.LittleEndian.Uint16(word[2:4])) + 1,
			run:         runMap != nil && runMap[i/8]&(1<<uint(i%8)) != 0,
		}
		if i > 0 && descriptors[i].key <= previous {
			return nil, moerr.NewInternalErrorNoCtx("roaring bitmap container keys are not increasing")
		}
		previous = descriptors[i].key
		total += uint64(descriptors[i].cardinality)
		if total > math.MaxInt {
			return nil, mpool.ErrAllocationAllocatorLimit
		}
	}
	if runMap == nil || count >= bitmapNoOffsetThreshold {
		for range count {
			if _, err = readBitmapWord(r, 4); err != nil {
				return nil, err
			}
		}
	}
	values, err := makeAccountedScratch[uint32](allocation, mp, int(total))
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success && cap(values) > 0 {
			mpool.FreeSlice(mp, values)
		}
	}()
	position := 0
	for _, descriptor := range descriptors {
		prefix := uint32(descriptor.key) << 16
		switch {
		case descriptor.run:
			word, err = readBitmapWord(r, 2)
			if err != nil {
				return nil, err
			}
			runs := int(binary.LittleEndian.Uint16(word[:2]))
			produced := 0
			previousEnd := -1
			for range runs {
				word, err = readBitmapWord(r, 4)
				if err != nil {
					return nil, err
				}
				start := int(binary.LittleEndian.Uint16(word[:2]))
				length := int(binary.LittleEndian.Uint16(word[2:4])) + 1
				if start <= previousEnd || produced > int(descriptor.cardinality)-length {
					return nil, moerr.NewInternalErrorNoCtx("invalid roaring run container")
				}
				for value := start; value < start+length; value++ {
					values[position] = prefix | uint32(value)
					position++
				}
				produced += length
				previousEnd = start + length - 1
			}
			if produced != int(descriptor.cardinality) {
				return nil, moerr.NewInternalErrorNoCtx("roaring run cardinality mismatch")
			}
		case descriptor.cardinality <= 4096:
			previousValue := -1
			for range descriptor.cardinality {
				word, err = readBitmapWord(r, 2)
				if err != nil {
					return nil, err
				}
				value := int(binary.LittleEndian.Uint16(word[:2]))
				if value <= previousValue {
					return nil, moerr.NewInternalErrorNoCtx("roaring array values are not increasing")
				}
				values[position] = prefix | uint32(value)
				position++
				previousValue = value
			}
		default:
			produced := uint32(0)
			for bitmapWord := 0; bitmapWord < 1024; bitmapWord++ {
				word, err = readBitmapWord(r, 8)
				if err != nil {
					return nil, err
				}
				valueBits := binary.LittleEndian.Uint64(word[:])
				produced += uint32(bits.OnesCount64(valueBits))
				for valueBits != 0 {
					bit := bits.TrailingZeros64(valueBits)
					values[position] = prefix | uint32(bitmapWord*64+bit)
					position++
					valueBits &= valueBits - 1
				}
			}
			if produced != descriptor.cardinality {
				return nil, moerr.NewInternalErrorNoCtx("roaring bitmap cardinality mismatch")
			}
		}
	}
	if position != len(values) {
		return nil, moerr.NewInternalErrorNoCtx("roaring bitmap decoded cardinality mismatch")
	}
	success = true
	return values, nil
}

func mergeAccountedBitmapWire(data []byte, target *bmp) error {
	if target == nil || target.allocation == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	total, err := scanAccountedBitmapWire(data, nil)
	if err != nil {
		return err
	}
	if total > math.MaxInt-len(target.values) {
		return mpool.ErrAllocationAllocatorLimit
	}
	// Admission reserved the final backing storage. Decode into the unpublished
	// tail, restoring the old logical length if a direct caller skipped
	// preflight and supplied a malformed wire.
	start := len(target.values)
	if start+total > cap(target.values) {
		return mpool.ErrAllocationAccountInvariant
	}
	target.values = target.values[:start+total]
	if _, err = scanAccountedBitmapWire(data, target.values[start:]); err != nil {
		target.values = target.values[:start]
		return err
	}
	target.normalize()
	return nil
}

func (b *bmp) MarshaledSize() int {
	if b.allocation == nil {
		return int(b.legacy.GetSerializedSizeInBytes())
	}
	containers := 0
	previous := uint32(math.MaxUint32)
	for _, value := range b.values {
		key := value >> 16
		if key != previous {
			containers++
			previous = key
		}
	}
	size := 8 + containers*8
	start := 0
	for start < len(b.values) {
		key := b.values[start] >> 16
		end := start + 1
		for end < len(b.values) && b.values[end]>>16 == key {
			end++
		}
		if end-start > 4096 {
			size += 8192
		} else {
			size += (end - start) * 2
		}
		start = end
	}
	return size
}

func writeBitmapFull(writer io.Writer, value []byte) error {
	n, err := writer.Write(value)
	if err == nil && n != len(value) {
		return io.ErrShortWrite
	}
	return err
}

func (b *bmp) MarshalTo(writer io.Writer) error {
	if b == nil || writer == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if b.allocation == nil {
		_, err := b.legacy.WriteTo(writer)
		return err
	}
	containers := 0
	previous := uint32(math.MaxUint32)
	for _, value := range b.values {
		key := value >> 16
		if key != previous {
			containers++
			previous = key
		}
	}
	var word [8]byte
	binary.LittleEndian.PutUint32(word[:4], 12346)
	binary.LittleEndian.PutUint32(word[4:], uint32(containers))
	if err := writeBitmapFull(writer, word[:]); err != nil {
		return err
	}
	start := 0
	offset := 8 + containers*8
	for start < len(b.values) {
		key := b.values[start] >> 16
		end := start + 1
		for end < len(b.values) && b.values[end]>>16 == key {
			end++
		}
		binary.LittleEndian.PutUint16(word[:2], uint16(key))
		binary.LittleEndian.PutUint16(word[2:4], uint16(end-start-1))
		if err := writeBitmapFull(writer, word[:4]); err != nil {
			return err
		}
		start = end
	}
	start = 0
	for start < len(b.values) {
		key := b.values[start] >> 16
		end := start + 1
		for end < len(b.values) && b.values[end]>>16 == key {
			end++
		}
		binary.LittleEndian.PutUint32(word[:4], uint32(offset))
		if err := writeBitmapFull(writer, word[:4]); err != nil {
			return err
		}
		if end-start > 4096 {
			offset += 8192
		} else {
			offset += (end - start) * 2
		}
		start = end
	}
	start = 0
	for start < len(b.values) {
		key := b.values[start] >> 16
		end := start + 1
		for end < len(b.values) && b.values[end]>>16 == key {
			end++
		}
		if end-start <= 4096 {
			for _, value := range b.values[start:end] {
				binary.LittleEndian.PutUint16(word[:2], uint16(value))
				if err := writeBitmapFull(writer, word[:2]); err != nil {
					return err
				}
			}
		} else {
			position := start
			for bitmapWord := 0; bitmapWord < 1024; bitmapWord++ {
				bits := uint64(0)
				for position < end &&
					int(uint16(b.values[position]))>>6 == bitmapWord {
					bits |= uint64(1) << (uint16(b.values[position]) & 63)
					position++
				}
				binary.LittleEndian.PutUint64(word[:], bits)
				if err := writeBitmapFull(writer, word[:]); err != nil {
					return err
				}
			}
		}
		start = end
	}
	return nil
}

func (b *bmp) Free() {
	if b == nil {
		return
	}
	if cap(b.values) > 0 {
		mpool.FreeSlice(b.mp, b.values)
	}
	b.values = nil
	b.legacy = nil
}

func (e *bmpExecCommon) batchMerge(other *bmpExecCommon, offset int, groups []uint64) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x1, y1 := e.getXY(group - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		if other.state[x2].mobs[y2] == nil {
			continue
		}
		if e.state[x1].mobs[y1] == nil && e.allocation == nil {
			e.state[x1].mobs[y1] = other.state[x2].mobs[y2]
			other.state[x2].mobs[y2] = nil
		} else {
			if e.state[x1].mobs[y1] == nil {
				var err error
				e.state[x1].mobs[y1], err = makeBmp(e.mp, e.allocation)
				if err != nil {
					return err
				}
			}
			mob1 := e.state[x1].mobs[y1].(*bmp)
			mob2 := other.state[x2].mobs[y2].(*bmp)
			if err := mob1.union(mob2); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *bmpExecCommon) flush(typ types.Type) (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(e.state))
	var output *mpool.AccountedBuffer
	if e.allocation != nil {
		var err error
		output, err = e.allocation.newArgumentBuffer(e.mp)
		if err != nil {
			return nil, err
		}
		defer output.Free()
	}
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(e.mp)
				}
			}
		}
	}()
	for i, st := range e.state {
		var err error
		vecs[i], err = e.allocation.newVector(typ)
		if err != nil {
			return nil, err
		}
		if err := vecs[i].PreExtend(int(st.length), e.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			if st.mobs[j] == nil {
				if err := vector.AppendNull(vecs[i], e.mp); err != nil {
					return nil, err
				}
			} else {
				mob := st.mobs[j].(*bmp)
				var bs []byte
				if output != nil {
					output.Reset()
					if err := mob.MarshalTo(output); err != nil {
						return nil, err
					}
					bs = output.Bytes()
				} else {
					var err error
					bs, err = mob.MarshalBinary()
					if err != nil {
						return nil, err
					}
				}
				if err := vector.AppendBytes(vecs[i], bs, false, e.mp); err != nil {
					return nil, err
				}
			}
		}
	}
	return vecs, nil
}

type bmpConstructExec struct {
	bmpExecCommon
}

func (e *bmpConstructExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return e.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (e *bmpConstructExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return e.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (e *bmpConstructExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		logicalRow := offset + i
		row := logicalRow
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		} else {
			x, y := e.getXY(group - 1)
			value := vector.GetFixedAtNoTypeCheck[uint64](vectors[0], row)
			if e.state[x].mobs[y] == nil {
				e.state[x].mobs[y], _ = makeBmp(e.mp, e.allocation)
			}
			mob := e.state[x].mobs[y].(*bmp)
			if err := mob.add(uint32(value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *bmpConstructExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return e.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (e *bmpConstructExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bmpConstructExec)
	return e.batchMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpConstructExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (e *bmpConstructExec) Flush() ([]*vector.Vector, error) {
	return e.flush(types.T_varbinary.ToType())
}

type bmpOrExec struct {
	bmpExecCommon
}

func (e *bmpOrExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return e.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (e *bmpOrExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return e.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (e *bmpOrExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		logicalRow := offset + i
		row := logicalRow
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		} else {
			x, y := e.getXY(group - 1)
			bs := vectors[0].GetBytesAt(row)
			if e.state[x].mobs[y] == nil {
				e.state[x].mobs[y], _ = makeBmp(e.mp, e.allocation)
			}
			mob := e.state[x].mobs[y].(*bmp)

			if e.allocation != nil {
				if err := mergeAccountedBitmapWire(bs, mob); err != nil {
					return err
				}
			} else {
				var mob2 bmp
				if err := mob2.UnmarshalBinary(bs); err != nil {
					return err
				}
				if err := mob.unionLegacy(mob2.legacy); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (e *bmpOrExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return e.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (e *bmpOrExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bmpOrExec)
	return e.batchMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpOrExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (e *bmpOrExec) Flush() ([]*vector.Vector, error) {
	return e.flush(types.T_varbinary.ToType())
}

func makeBmpOrExec(mp *mpool.MPool, id int64, param types.Type) *bmpOrExec {
	var exec bmpOrExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{param},
		retType:                  param,
		stateTypes:               nil,
		emptyNull:                true,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeBmpMarshalerUnmarshaler,
		boundedOpaqueState:       true,
	}
	return &exec
}

func makeBmpConstructExec(mp *mpool.MPool, id int64, param types.Type) *bmpConstructExec {
	var exec bmpConstructExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{param},
		retType:                  types.T_varbinary.ToType(),
		stateTypes:               nil,
		emptyNull:                true,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeBmpMarshalerUnmarshaler,
		boundedOpaqueState:       true,
	}
	return &exec
}
