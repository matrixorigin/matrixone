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

package aggexec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	io "io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	AggBatchSize                = 8192
	aggBatchSizeShift           = 13 // log2(AggBatchSize)
	aggBatchSizeMask            = AggBatchSize - 1
	kAggArgArenaSize            = 512 * 1024
	kAggArgPrefixSz             = 2
	kAggArgOrdinalSz            = 4
	magicNumber                 = uint64(0xdeadbeefbeefdead)
	spillMagicNumber            = uint64(0x4752505350494c4c) // "GRPSPILL"
	aggBinaryStringTrailerMagic = uint64(0x4147474253545231)
	aggStringDomainTrailerMagic = uint64(0x4147474253545232)
	aggStringStateTrailerMagic  = uint64(0x4147474253545233)
)

var _ [0]struct{} = [AggBatchSize & aggBatchSizeMask]struct{}{}       // mask == size-1
var _ [1]struct{} = [1 << aggBatchSizeShift / AggBatchSize]struct{}{} // shift matches size

type MarshalerUnmarshaler interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
	UnmarshalFromReader(io.Reader) error
}

// boundedMarshalerUnmarshaler is an opaque aggregate state whose encoded
// representation can be written without first materializing a second copy.
// Implementations admitted by aggInfo.boundedOpaqueState must also own every
// data-scaled allocation through the aggregate allocation account.
type boundedMarshalerUnmarshaler interface {
	MarshalerUnmarshaler
	MarshaledSize() int
	MarshalTo(io.Writer) error
}

type freeableMarshalerUnmarshaler interface {
	Free()
}

func writeBoundedOpaqueState(
	state MarshalerUnmarshaler,
	writer io.Writer,
	aggregateID int64,
) error {
	stream, ok := state.(boundedMarshalerUnmarshaler)
	if !ok {
		return moerr.NewInternalErrorNoCtxf(
			"aggregate %d opaque state is not streamable", aggregateID)
	}
	size := stream.MarshaledSize()
	if size < 0 || uint64(size) > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtxf(
			"aggregate %d opaque state size %d is invalid", aggregateID, size)
	}
	if err := types.WriteInt32(writer, int32(size)); err != nil {
		return err
	}
	return stream.MarshalTo(writer)
}

type aggInfo struct {
	aggId              int64
	isDistinct         bool
	argTypes           []types.Type
	retType            types.Type
	stateTypes         []types.Type
	emptyNull          bool
	saveArg            bool
	opaqueArg          bool
	boundedOpaqueState bool
	// preserveDistinctInputOrder stores the first-seen ordinal in each DISTINCT
	// saved-argument node. The wire format remains a sequence of keys in that
	// order, so older partial-state readers see the same payload representation.
	preserveDistinctInputOrder bool
	// preserveStringMetadata keeps row-level string domain/source metadata with
	// saved aggregate arguments. Aggregates whose result is a selected input
	// value (currently discrete percentile) need this because saveArg otherwise
	// retains only raw bytes and loses the selected value's runtime semantics.
	preserveStringMetadata bool
	// stableEmptyOpaqueState preserves an aggregate's historical partial-result
	// representation when its resident implementation can now omit empty state.
	// Private spill records deliberately keep the compact zero-size marker.
	stableEmptyOpaqueState   func(io.Writer) error
	makeMarshalerUnmarshaler func(mp *mpool.MPool, allocation *AllocationAccount) (MarshalerUnmarshaler, error)
}

func (a *aggInfo) String() string {
	return fmt.Sprintf("aggId: %d, isDistinct: %t, argTypes: %v, retType: %v, emptyNull: %t", a.aggId, a.isDistinct, a.argTypes, a.retType, a.emptyNull)
}

func (a *aggInfo) AggID() int64 {
	return a.aggId
}

func (a *aggInfo) IsDistinct() bool {
	return a.isDistinct
}

func (a *aggInfo) TypesInfo() ([]types.Type, types.Type) {
	return a.argTypes, a.retType
}

func (a *aggInfo) usesOpaqueArgEncoding() bool {
	return a.opaqueArg || len(a.argTypes) != 1 || !a.argTypes[0].IsFixedLen()
}

type aggState struct {
	length     int32
	capacity   int32
	allocation *AllocationAccount
	// distinctKeyWidth enables the exact fixed-width deferred DISTINCT state.
	// When enabled, the index is the membership/iteration source and the
	// skiplist remains available for legacy/fallback states. A small hard-account
	// merge destination may disable the empty optional index before publication.
	distinctKeyWidth      int
	distinctFixedDeferred bool
	distinctIndex         distinctFixedIndex
	// vecs are for agg state.
	vecs []*vector.Vector
	// MarshalerUnmarshaler, for state entries.
	// Note that using this, means we pretty much give up memory management
	// for the state entries.
	mobs []MarshalerUnmarshaler

	// argbuf is buffer to the arena for skiplist
	argCnt []uint32
	argbuf []byte
	argSkl *arenaskl.Skiplist
	// argScratch is reusable physical storage for key construction and spill
	// decode. It avoids data-scaled Go byte slices on row-frequency paths.
	argScratch []byte
	// boundedArgumentGrowth is enabled after exact DISTINCT ownership moves to
	// Group's partition spool. Subsequent resident state is only a work set, so
	// arena growth follows bounded 64 KiB steps instead of retaining speculative
	// 512 KiB/global-NDV capacity.
	boundedArgumentGrowth bool
}

func (ag *aggState) init(
	mp *mpool.MPool,
	l, c int32,
	info *aggInfo,
	setNulls bool,
) error {
	return ag.initWithAllocation(mp, l, c, info, setNulls, nil)
}

func (ag *aggState) initWithAllocation(
	mp *mpool.MPool,
	l, c int32,
	info *aggInfo,
	setNulls bool,
	allocation *AllocationAccount,
) error {
	if c <= 0 || c > AggBatchSize {
		return moerr.NewInternalErrorNoCtxf("invalid length or capacity: %d, %d", l, c)
	}
	if l != 0 && l != c {
		return moerr.NewInternalErrorNoCtxf("invalid length or capacity: %d, %d", l, c)
	}
	ag.length = l
	ag.capacity = c
	ag.allocation = allocation
	ag.distinctKeyWidth = distinctFixedIndexWidth(info, int(c))
	ag.distinctFixedDeferred = ag.distinctKeyWidth > 0
	ag.distinctIndex.groupLimit = int(c)

	var err error
	if !info.saveArg {
		ag.vecs = make([]*vector.Vector, len(info.stateTypes))
		for i, typ := range info.stateTypes {
			ag.vecs[i], err = allocation.newVector(typ)
			if err != nil {
				for j := 0; j < i; j++ {
					ag.vecs[j].Free(mp)
				}
				ag.vecs = nil
				return err
			}
			if err = ag.vecs[i].PreExtend(int(c), mp); err != nil {
				for j := 0; j <= i; j++ {
					ag.vecs[j].Free(mp)
				}
				ag.vecs = nil
				return err
			}
			if info.emptyNull && setNulls {
				if err = ag.vecs[i].PreExtendNulls(int(c), mp); err != nil {
					for j := 0; j <= i; j++ {
						ag.vecs[j].Free(mp)
					}
					ag.vecs = nil
					return err
				}
				ag.vecs[i].SetAllNulls(int(c))
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			ag.mobs = make([]MarshalerUnmarshaler, int(c))
		}
	} else {
		if ag.argCnt, err = allocation.makeArgumentCounts(mp, int(c)); err != nil {
			return err
		}

		bufsz := kAggArgArenaSize
		// A hard-account exact COUNT(DISTINCT) can spill its canonical keys.
		// Avoid speculatively retaining a 512 KiB arena per chunk before the
		// first drain. Normal pre-activation growth remains 512 KiB at a time;
		// the spill transition separately switches replacement work sets to
		// bounded 64 KiB growth.
		if allocation != nil && info.aggId == AggIdOfCountColumn &&
			info.isDistinct {
			bufsz = 64 * 1024
		}
		if c < 1024 {
			bufsz = 16 * 1024
		}

		if ag.argbuf, err = allocation.allocArgumentArena(mp, bufsz); err != nil {
			mpool.FreeSlice(mp, ag.argCnt)
			ag.argCnt = nil
			return err
		}
		arena := arenaskl.NewArena(ag.argbuf)
		ag.argSkl = arenaskl.NewSkiplist(arena, bytes.Compare)
	}
	return nil
}

func (ag *aggState) grow(mp *mpool.MPool, more int32, expandLen bool) (int32, int32, error) {
	canAdd := int32(ag.capacity - ag.length)
	var toAdd int32

	if more <= canAdd {
		canAdd = more
	} else {
		toAdd = more - canAdd
	}

	if !expandLen || canAdd == 0 {
		return canAdd, toAdd, nil
	}

	// Reserve every row-parallel allocation before publishing the group count.
	// Successful capacity growth is reusable if a later vector fails, while all
	// logical lengths remain unchanged and the caller receives the OOM.
	for _, vec := range ag.vecs {
		if err := vec.PreExtend(int(canAdd), mp); err != nil {
			return 0, more, err
		}
	}
	ag.length += canAdd
	for _, vec := range ag.vecs {
		vec.SetLength(int(ag.length))
	}

	return canAdd, toAdd, nil
}

func (ag *aggState) writeStateArg(
	mp *mpool.MPool,
	i int32,
	writer io.Writer,
	info *aggInfo,
) error {
	if err := types.WriteUint32(writer, ag.argCnt[i]); err != nil {
		return err
	}
	if ag.argCnt[i] != 0 {
		// open iterator and write to buf
		xcnt := 0
		var lkb, ukb [kAggArgPrefixSz]byte
		lk := lkb[:]
		uk := ukb[:]
		binary.BigEndian.PutUint16(lk, uint16(i))
		binary.BigEndian.PutUint16(uk, uint16(i+1))
		if info.preserveDistinctInputOrder {
			err := ag.iterInputOrder(mp, uint16(i), func(k []byte) error {
				if err := types.WriteSizeBytes(k[kAggArgPrefixSz:], writer); err != nil {
					return err
				}
				xcnt++
				return nil
			})
			if err != nil {
				return err
			}
		} else if ag.distinctFixedDeferred {
			err := ag.iter(uint16(i), func(k []byte) error {
				value := k[kAggArgPrefixSz:]
				n, err := writer.Write(value)
				if err != nil {
					return err
				}
				if n != len(value) {
					return io.ErrShortWrite
				}
				xcnt++
				return nil
			})
			if err != nil {
				return err
			}
		} else {
			it := ag.argSkl.NewIter(lk, uk)
			defer it.Close()
			if !info.usesOpaqueArgEncoding() {
				for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
					/*
						checkI := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
						if checkI != uint16(i) {
							panic(moerr.NewInternalErrorNoCtxf("writeStateArg: mismatch i: %d != %d", checkI, i))
						}
					*/
					value := k[kAggArgPrefixSz:]
					n, err := writer.Write(value)
					if err != nil {
						return err
					}
					if n != len(value) {
						return io.ErrShortWrite
					}
					xcnt++
				}
			} else {
				for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
					/*
						checkI := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
						if checkI != uint16(i) {
							panic(moerr.NewInternalErrorNoCtxf("writeStateArg: mismatch i: %d != %d", checkI, i))
						}
					*/
					if err := types.WriteSizeBytes(k[kAggArgPrefixSz:], writer); err != nil {
						return err
					}
					xcnt++
				}
			}
		}

		if int(ag.argCnt[i]) != xcnt {
			return moerr.NewInternalErrorNoCtxf(
				"writeStateArg: mismatch count: %d != %d", xcnt, ag.argCnt[i])
		}
	}
	return nil
}

func (ag *aggState) readStateArg(mp *mpool.MPool, i int32, r io.Reader, info *aggInfo) error {
	var err error
	if ag.argCnt[i], err = types.ReadUint32(r); err != nil {
		return err
	}
	if ag.argCnt[i] == 0 {
		return nil
	}
	// Read directly into reusable, allocation-accounted key scratch. The
	// skiplist copies each key into its own arena before the next iteration.
	opaqueArg := info.usesOpaqueArgEncoding()
	var fixedKey []byte
	if !opaqueArg {
		fixedLen := int(info.argTypes[0].GetSize())
		keySize := kAggArgPrefixSz + fixedLen
		if !info.isDistinct {
			keySize += kAggArgOrdinalSz
		}
		fixedKey, err = ag.resizeArgScratch(mp, keySize)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(fixedKey[:kAggArgPrefixSz], uint16(i))
	}
	// writeStateArg emits non-order-preserving arguments in skiplist key order.
	// Keep the insertion cursor across that ordered stream so rebuilding a large
	// partial DISTINCT state does not search the skiplist from the root for every
	// argument. Inserter revalidates its cached splice if it sees an older or
	// non-canonical wire stream.
	var inserter arenaskl.Inserter
	for ui := uint32(0); ui < ag.argCnt[i]; ui++ {
		if !opaqueArg {
			if _, err = io.ReadFull(r, fixedKey[kAggArgPrefixSz:]); err != nil {
				return err
			}
			if info.preserveDistinctInputOrder {
				var ordinal [kAggArgOrdinalSz]byte
				binary.BigEndian.PutUint32(ordinal[:], ui)
				err = ag.insertArgValue(mp, fixedKey, ordinal[:])
			} else {
				err = ag.insertArgValueWithInserter(mp, fixedKey, nil, &inserter)
			}
			if err != nil {
				return err
			}
		} else {
			wireSize, err := types.ReadInt32AsInt(r)
			if err != nil {
				return err
			}
			if wireSize < 0 || wireSize > math.MaxInt-kAggArgPrefixSz {
				return moerr.NewInvalidInputNoCtx("invalid aggregate argument size")
			}
			kbuf, err := ag.resizeArgScratch(mp, kAggArgPrefixSz+wireSize)
			if err != nil {
				return err
			}
			binary.BigEndian.PutUint16(kbuf[:kAggArgPrefixSz], uint16(i))
			if _, err = io.ReadFull(r, kbuf[kAggArgPrefixSz:]); err != nil {
				return err
			}
			if info.preserveDistinctInputOrder {
				var ordinal [kAggArgOrdinalSz]byte
				binary.BigEndian.PutUint32(ordinal[:], ui)
				err = ag.insertArgValue(mp, kbuf, ordinal[:])
			} else {
				err = ag.insertArgValueWithInserter(mp, kbuf, nil, &inserter)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ag *aggState) writeStateToBuf(mp *mpool.MPool, info *aggInfo, flags []uint8, writer io.Writer) error {
	if len(flags) > int(ag.length) {
		return moerr.NewInvalidInputNoCtxf(
			"aggregate selection length %d exceeds state row count %d",
			len(flags), ag.length)
	}
	var cnt int32
	for i := range flags {
		if flags[i] != 0 {
			cnt += 1
		}
	}

	if err := types.WriteInt32(writer, cnt); err != nil {
		return err
	}
	if cnt == 0 {
		return nil
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			err := func() error {
				bufVec, err := ag.allocation.newVector(*vec.GetType())
				if err != nil {
					return err
				}
				defer bufVec.Free(mp)
				if err := bufVec.UnionBatch(vec, 0, int(cnt), flags, mp); err != nil {
					return err
				}
				if err := bufVec.MarshalBinaryTo(writer); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}

		if info.makeMarshalerUnmarshaler != nil {
			for i := range flags {
				if flags[i] != 0 {
					if ag.mobs[i] == nil {
						if err := writeStableEmptyOpaqueState(info, writer); err != nil {
							return err
						}
					} else if info.boundedOpaqueState {
						if err := writeBoundedOpaqueState(
							ag.mobs[i], writer, info.aggId); err != nil {
							return err
						}
					} else {
						if bs, err := ag.mobs[i].MarshalBinary(); err != nil {
							return err
						} else {
							if err := types.WriteSizeBytes(bs, writer); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	} else {
		if ag.argSkl == nil {
			return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
		}
		for i := range flags {
			if flags[i] != 0 {
				if err := ag.writeStateArg(mp, int32(i), writer, info); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ag *aggState) writeSpillStateRows(
	mp *mpool.MPool,
	info *aggInfo,
	rows []int32,
	writer io.Writer,
) (int32, error) {
	if len(rows) > AggBatchSize {
		return 0, moerr.NewInvalidInputNoCtxf(
			"aggregate spill selection length %d exceeds work unit %d",
			len(rows), AggBatchSize)
	}
	for _, row := range rows {
		if row < 0 || row >= ag.length {
			return 0, moerr.NewInvalidInputNoCtxf(
				"aggregate spill row %d exceeds state row count %d",
				row, ag.length)
		}
	}
	cnt := int32(len(rows))
	if err := types.WriteInt32(writer, cnt); err != nil {
		return 0, err
	}
	if cnt == 0 {
		return 0, nil
	}
	if info.makeMarshalerUnmarshaler != nil && !info.boundedOpaqueState {
		return 0, moerr.NewNotSupportedNoCtxf(
			"aggregate %d has opaque spill state", info.aggId)
	}
	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.MarshalSelectedRowsTo(writer, rows); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for _, row := range rows {
				if ag.mobs[row] == nil {
					if err := types.WriteInt32(writer, 0); err != nil {
						return 0, err
					}
					continue
				}
				if err := writeBoundedOpaqueState(
					ag.mobs[row], writer, info.aggId); err != nil {
					return 0, err
				}
			}
		}
		return cnt, nil
	}
	if ag.argSkl == nil {
		return 0, moerr.NewInternalErrorNoCtx("argSkl is not initialized")
	}
	for _, row := range rows {
		if err := ag.writeStateArg(mp, row, writer, info); err != nil {
			return 0, err
		}
	}
	return cnt, nil
}

func (ag *aggState) readSpillState(
	mp *mpool.MPool,
	reader io.Reader,
	info *aggInfo,
	allocation *AllocationAccount,
) (int32, error) {
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return 0, err
	}
	if cnt < 0 || cnt > AggBatchSize {
		return 0, moerr.NewInvalidInputNoCtxf(
			"invalid aggregate spill count %d", cnt)
	}
	if info.makeMarshalerUnmarshaler != nil && !info.boundedOpaqueState {
		return 0, moerr.NewNotSupportedNoCtxf(
			"aggregate %d has opaque spill state", info.aggId)
	}
	if cnt == 0 {
		ag.free(mp)
		return 0, nil
	}

	// A spill record contains at most one bounded group work unit. Reuse its
	// physical vectors when possible; otherwise initialize one exact-capacity
	// state under the incoming aggregate's recovery allocation selection.
	reuse := !info.saveArg && len(ag.vecs) == len(info.stateTypes) &&
		ag.capacity >= cnt
	if !reuse {
		ag.free(mp)
		if err := ag.initWithAllocation(mp, 0, cnt, info, false, allocation); err != nil {
			return 0, err
		}
	}
	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.UnmarshalSelectedRowsFrom(reader, int(cnt), mp); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for row := range cnt {
				sz, err := types.ReadInt32(reader)
				if err != nil {
					return 0, err
				}
				if sz < 0 {
					return 0, moerr.NewInvalidInputNoCtxf(
						"invalid aggregate opaque spill state size %d", sz)
				}
				if sz == 0 {
					continue
				}
				if ag.mobs[row], err = info.makeMarshalerUnmarshaler(mp, allocation); err != nil {
					return 0, err
				}
				limited := &io.LimitedReader{R: reader, N: int64(sz)}
				if err := ag.mobs[row].UnmarshalFromReader(limited); err != nil {
					return 0, err
				}
				if limited.N != 0 {
					return 0, io.ErrUnexpectedEOF
				}
			}
		}
		ag.length = cnt
		return cnt, nil
	}
	for row := range cnt {
		if err := ag.readStateArg(mp, row, reader, info); err != nil {
			return 0, err
		}
	}
	ag.length = cnt
	return cnt, nil
}

func (ag *aggState) writeAllStatesToBuf(
	mp *mpool.MPool,
	writer io.Writer,
	info *aggInfo,
) error {
	if err := types.WriteInt32(writer, ag.length); err != nil {
		return err
	}
	if ag.length == 0 {
		return nil
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.MarshalBinaryTo(writer); err != nil {
				return err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for _, entry := range ag.mobs[:ag.length] {
				/*
					for gap between groups like:
					group 0 , group 1(gap), group 2.

					group 0 and group 2 have data.
					group 1 does not have data. there is no marshal for group 1.
				*/
				if entry == nil {
					if err := writeStableEmptyOpaqueState(info, writer); err != nil {
						return err
					}
				} else if info.boundedOpaqueState {
					if err := writeBoundedOpaqueState(
						entry, writer, info.aggId); err != nil {
						return err
					}
				} else {
					if bs, err := entry.MarshalBinary(); err != nil {
						return err
					} else {
						if err := types.WriteSizeBytes(bs, writer); err != nil {
							return err
						}
					}
				}
			}
		}
	} else {
		if ag.argSkl == nil {
			return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
		}
		for i := range ag.length {
			if err := ag.writeStateArg(mp, int32(i), writer, info); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeStableEmptyOpaqueState(info *aggInfo, writer io.Writer) error {
	if info != nil && info.stableEmptyOpaqueState != nil {
		return info.stableEmptyOpaqueState(writer)
	}
	return types.WriteSizeBytes(nil, writer)
}

func (ag *aggState) readState(
	mp *mpool.MPool,
	reader io.Reader,
	info *aggInfo,
) (int32, error) {
	return ag.readStateWithAllocation(mp, reader, info, nil)
}

func (ag *aggState) readStateWithAllocation(
	mp *mpool.MPool,
	reader io.Reader,
	info *aggInfo,
	allocation *AllocationAccount,
) (int32, error) {
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return 0, err
	}
	if cnt == 0 {
		if !info.saveArg && info.makeMarshalerUnmarshaler == nil {
			ag.length = 0
			for _, vec := range ag.vecs {
				if vec != nil {
					vec.CleanOnlyData()
				}
			}
		} else {
			ag.free(mp)
			ag.length = 0
			ag.capacity = 0
		}
		return 0, nil
	}

	if cnt < 0 || cnt > AggBatchSize {
		return 0, moerr.NewInternalErrorNoCtxf("invalid count: %d", cnt)
	}
	reuseVectors := !info.saveArg && info.makeMarshalerUnmarshaler == nil &&
		len(ag.vecs) == len(info.stateTypes) && ag.capacity >= cnt
	if reuseVectors {
		ag.length = cnt
		for _, vec := range ag.vecs {
			if vec != nil {
				vec.CleanOnlyData()
			}
		}
	} else {
		ag.free(mp)
		if err := ag.initWithAllocation(mp, cnt, cnt, info, false, allocation); err != nil {
			return 0, err
		}
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.UnmarshalWithReader(reader, mp); err != nil {
				return 0, err
			}
			if vec.Length() != int(cnt) {
				return 0, moerr.NewInvalidInputNoCtxf(
					"aggregate state vector row count %d does not match %d",
					vec.Length(), cnt)
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for i := range cnt {
				sz, err := types.ReadInt32(reader)
				if err != nil {
					return 0, err
				}
				if sz < 0 {
					return 0, moerr.NewInvalidInputNoCtxf(
						"invalid aggregate opaque state size %d", sz)
				}
				if sz > 0 {
					//only need marshal for size > 0.
					if ag.mobs[i], err = info.makeMarshalerUnmarshaler(mp, allocation); err != nil {
						return 0, err
					}
					lr := &io.LimitedReader{R: reader, N: int64(sz)}
					if err := ag.mobs[i].UnmarshalFromReader(lr); err != nil {
						return 0, err
					}
					remaining := lr.N
					n, copyErr := io.Copy(io.Discard, lr)
					if copyErr != nil {
						return 0, copyErr
					}
					if n != remaining {
						return 0, io.ErrUnexpectedEOF
					}
					if n > 0 {
						return 0, moerr.NewInternalErrorNoCtxf("mob unmarshal did not consume all bytes: %d remaining", n)
					}
				}
			}
		}
	} else {
		for i := range cnt {
			if err := ag.readStateArg(mp, int32(i), reader, info); err != nil {
				return 0, err
			}
		}
	}

	return cnt, nil
}

// appendFromStateArg appends the state from other aggState (starting from offset)
func (ag *aggState) appendFromStateArg(mp *mpool.MPool, otherOffset int32, other *aggState, info *aggInfo) (int32, error) {
	// first decide how many we can append
	space := int32(ag.capacity - ag.length)
	if space == 0 {
		// no space to append, return the original offset and caller will append to the next group.
		return otherOffset, nil
	}

	start := otherOffset
	end := start + space
	if end > other.length {
		end = other.length
	}

	if !info.saveArg {
		for i := range ag.vecs {
			if err := ag.vecs[i].UnionBatch(other.vecs[i], int64(otherOffset), int(end-start), nil, mp); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for i := int32(0); i < end-start; i++ {
				ag.mobs[ag.length+i] = other.mobs[otherOffset+i]
				other.mobs[otherOffset+i] = nil
			}
		}
		ag.length += end - start
	} else {
		if info.isDistinct && ag.distinctFixedDeferred && other.distinctFixedDeferred &&
			ag.distinctKeyWidth == other.distinctKeyWidth {
			for i := start; i < end; i++ {
				targetY := uint16(ag.length)
				ag.argCnt[targetY] = 0
				added, err := ag.distinctIndex.mergeInto(
					mp, ag.allocation, targetY, &other.distinctIndex, uint16(i))
				if err != nil {
					return 0, err
				}
				if added > math.MaxUint32-ag.argCnt[targetY] {
					return 0, moerr.NewInternalErrorNoCtx(
						"agg appendFromStateArg: too many distinct arguments")
				}
				ag.argCnt[targetY] += added
				ag.length++
			}
		} else {
			for i := start; i < end; i++ {
				ag.argCnt[ag.length] = other.argCnt[i]
				var lkb, ukb [kAggArgPrefixSz]byte
				lk := lkb[:]
				uk := ukb[:]
				binary.BigEndian.PutUint16(lk, uint16(i))
				binary.BigEndian.PutUint16(uk, uint16(i+1))
				it := other.argSkl.NewIter(lk, uk)
				for ok, k, value := it.SeekGE(lk); ok; ok, k, value = it.Next() {
					kcpy, err := ag.resizeArgScratch(mp, len(k))
					if err != nil {
						it.Close()
						return 0, err
					}
					copy(kcpy, k)
					binary.BigEndian.PutUint16(kcpy[:kAggArgPrefixSz], uint16(ag.length))
					if err := ag.insertArgValue(mp, kcpy, value); err != nil {
						it.Close()
						return 0, err
					}
				}
				it.Close()
				ag.length++
			}
		}
	}
	return end, nil
}

func (ag *aggState) resizeArgScratch(
	mp *mpool.MPool,
	length int,
) ([]byte, error) {
	if ag == nil || mp == nil || length < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if _, err := aggregateArgumentNodeSize(uint64(length), 0); err != nil {
		return nil, err
	}
	if cap(ag.argScratch) >= length {
		ag.argScratch = ag.argScratch[:length]
		return ag.argScratch, nil
	}
	next, err := ag.allocation.allocArgumentArena(mp, length)
	if err != nil {
		return nil, err
	}
	if cap(ag.argScratch) > 0 {
		mp.Free(ag.argScratch)
	}
	ag.argScratch = next
	return ag.argScratch, nil
}

func aggregateArgumentNodeSize(keySize, valueSize uint64) (uint64, error) {
	if keySize > math.MaxUint32 || valueSize > math.MaxUint32 {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	need := arenaskl.MaxNodeSize(uint32(keySize), uint32(valueSize))
	// Arena offsets and a complete node are both uint32-sized. NewArena and
	// newNode panic when those limits are exceeded, so reject the row as a
	// controlled allocation error before either is called.
	if need > math.MaxUint32 {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	return need, nil
}

func (ag *aggState) insertArg(mp *mpool.MPool, kbuf []byte) error {
	return ag.insertArgValue(mp, kbuf, nil)
}

func (ag *aggState) insertArgValue(mp *mpool.MPool, kbuf, value []byte) error {
	return ag.insertArgValueWithInserter(mp, kbuf, value, nil)
}

func (ag *aggState) insertArgValueWithInserter(
	mp *mpool.MPool,
	kbuf, value []byte,
	inserter *arenaskl.Inserter,
) error {
	if ag.argSkl == nil {
		return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
	}
	var (
		distinctIndexKey  uint64
		distinctIndexGrp  uint16
		distinctIndexUsed bool
	)
	if ag.distinctKeyWidth > 0 {
		var ok bool
		distinctIndexGrp, distinctIndexKey, ok = decodeDistinctFixedKey(kbuf, ag.distinctKeyWidth)
		if !ok && ag.distinctFixedDeferred {
			// A deferred fixed-width state has no skiplist source of truth:
			// iterators, merge, and spill read the index directly.  Falling back
			// to the empty compatibility skiplist for a malformed/retyped payload
			// would publish argCnt without publishing an index key and silently
			// lose the value at the next boundary.
			return mpool.ErrAllocationAccountInvariant
		}
		if ok {
			distinctIndexUsed = true
			duplicate, err := ag.distinctIndex.prepare(
				mp, ag.allocation, distinctIndexGrp, distinctIndexKey)
			if err != nil {
				return err
			}
			if duplicate {
				return arenaskl.ErrRecordExists
			}
			if ag.distinctFixedDeferred {
				return ag.distinctIndex.insert(distinctIndexGrp, distinctIndexKey)
			}
		}
	}
	publish := func(err error) error {
		if err == nil && distinctIndexUsed && !ag.distinctFixedDeferred {
			return ag.distinctIndex.insert(distinctIndexGrp, distinctIndexKey)
		}
		return err
	}
	if ag.allocation != nil {
		if uint64(len(kbuf)) > math.MaxUint32 ||
			uint64(len(value)) > math.MaxUint32 {
			return mpool.ErrAllocationAllocatorLimit
		}
		plan := arenaskl.MakeAddPlan(kbuf)
		consumed, trailing, ok := plan.ArenaFootprint(
			uint32(len(kbuf)), uint32(len(value)))
		if !ok || consumed > math.MaxUint64-trailing {
			return mpool.ErrAllocationAllocatorLimit
		}
		required := consumed + trailing
		used := uint64(ag.argSkl.Arena().Size())
		if used <= math.MaxUint64-required &&
			used+required <= uint64(ag.argSkl.Arena().Capacity()) {
			if inserter != nil {
				return publish(inserter.AddWithPlan(ag.argSkl, kbuf, value, plan))
			}
			return publish(ag.argSkl.AddWithPlan(kbuf, value, plan))
		}
		// Admission deliberately reserves no capacity for a DISTINCT key that is
		// already present. Confirm that case before growing; doing this only at a
		// capacity boundary keeps the common new-key path to one skiplist search.
		if ag.argSkl.Contains(kbuf) {
			return arenaskl.ErrRecordExists
		}
		// Relocate the offset-based nodes as one buffer under the state's selected
		// growth policy. Ordinary recovery grows geometrically; bounded DISTINCT
		// work sets retain their smaller increments without rebuilding every node.
		if err := ag.preflightArgumentCapacity(mp, required, 0); err != nil {
			return err
		}
		if inserter != nil {
			// GrowArena relocates the backing buffer, so cached node pointers no
			// longer refer to the current arena.
			*inserter = arenaskl.Inserter{}
			return publish(inserter.AddWithPlan(ag.argSkl, kbuf, value, plan))
		}
		return publish(ag.argSkl.AddWithPlan(kbuf, value, plan))
	}

	nodeSize, err := aggregateArgumentNodeSize(
		uint64(len(kbuf)), uint64(len(value)))
	if err != nil {
		return err
	}
	add := func(list *arenaskl.Skiplist) error {
		if inserter != nil {
			return inserter.Add(list, kbuf, value)
		}
		return list.Add(kbuf, value)
	}
	if err := add(ag.argSkl); err != arenaskl.ErrArenaFull {
		return publish(err)
	}

	// arena is full, we need to grow the arena. Grow by at least kAggArgArenaSize,
	// but if a single key (plus its skiplist node overhead) needs more than that —
	// e.g. a multi-column distinct key concatenating several large string args —
	// grow by enough to fit it, otherwise the retry below would still ErrArenaFull.
	grow := uint64(kAggArgArenaSize)
	if ag.boundedArgumentGrowth {
		grow = 64 * 1024
	}
	if nodeSize > grow {
		grow = nodeSize
	}
	current := uint64(len(ag.argbuf))
	if current > math.MaxUint32 || grow > math.MaxUint32-current ||
		current+grow > uint64(math.MaxInt) {
		return mpool.ErrAllocationAllocatorLimit
	}
	argBuf, err := ag.allocation.allocArgumentArena(
		mp,
		int(current+grow),
	)
	if err != nil {
		return err
	}
	newArena := arenaskl.NewArena(argBuf)
	newArgSkl := arenaskl.NewSkiplist(newArena, bytes.Compare)
	// move entries to new arena
	// I am pretty sure a realloc then fix a few pointers in skl should work, but
	// let's not do that for now, until the profiling shows this is a bottleneck.
	it := ag.argSkl.NewIter(nil, nil)
	for ok, k, oldValue := it.First(); ok; ok, k, oldValue = it.Next() {
		if err := newArgSkl.Add(k, oldValue); err != nil {
			it.Close()
			mp.Free(argBuf)
			return err
		}
	}
	it.Close()
	if inserter != nil {
		*inserter = arenaskl.Inserter{}
	}
	if err = add(newArgSkl); err != nil {
		mp.Free(argBuf)
		return err
	}

	oldArgBuf := ag.argbuf
	ag.argbuf = argBuf
	ag.argSkl = newArgSkl
	mp.Free(oldArgBuf)
	return publish(nil)
}

func (ag *aggState) fillArg(mp *mpool.MPool, y uint16, val []byte, distinct bool) error {
	return ag.fillArgPayload(mp, y, val, distinct)
}

const (
	aggStringMetadataMagic0     byte = 0xf1
	aggStringMetadataMagic1     byte = 0x53 // 'S'
	aggStringMetadataMagic2     byte = 0x4d // 'M'
	aggStringMetadataVersion    byte = 1
	aggStringMetadataHeaderSize      = 6
)

func (ag *aggState) fillArgWithStringMetadata(
	mp *mpool.MPool,
	y uint16,
	val []byte,
	domain types.RuntimeStringDomain,
	source types.StringSource,
	distinct bool,
) error {
	header := kAggArgPrefixSz
	if !distinct {
		header += kAggArgOrdinalSz
	}
	if len(val) > math.MaxInt-header-aggStringMetadataHeaderSize {
		return mpool.ErrAllocationAllocatorLimit
	}
	k, err := ag.resizeArgScratch(mp, header+aggStringMetadataHeaderSize+len(val))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(k[:kAggArgPrefixSz], y)
	payload := k[header:]
	payload[0] = aggStringMetadataMagic0
	payload[1] = aggStringMetadataMagic1
	payload[2] = aggStringMetadataMagic2
	payload[3] = aggStringMetadataVersion
	payload[4] = byte(domain)
	payload[5] = byte(source)
	copy(payload[aggStringMetadataHeaderSize:], val)
	if !distinct {
		binary.BigEndian.PutUint32(k[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz], ag.argCnt[y])
	}
	return ag.insertPreparedArg(mp, y, k, distinct)
}

func (ag *aggState) fillArgPayload(mp *mpool.MPool, y uint16, val []byte, distinct bool) error {
	header := kAggArgPrefixSz
	if !distinct {
		header += kAggArgOrdinalSz
	}
	if len(val) > math.MaxInt-header {
		return mpool.ErrAllocationAllocatorLimit
	}
	k, err := ag.resizeArgScratch(mp, header+len(val))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(k[:kAggArgPrefixSz], y)
	if distinct {
		copy(k[kAggArgPrefixSz:], val)
	} else {
		binary.BigEndian.PutUint32(k[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz], ag.argCnt[y])
		copy(k[kAggArgPrefixSz+kAggArgOrdinalSz:], val)
	}
	return ag.insertPreparedArg(mp, y, k, distinct)
}

func decodeAggStringMetadata(payload []byte) (
	raw []byte,
	domain types.RuntimeStringDomain,
	source types.StringSource,
	encoded bool,
	error error,
) {
	if len(payload) < aggStringMetadataHeaderSize ||
		payload[0] != aggStringMetadataMagic0 ||
		payload[1] != aggStringMetadataMagic1 ||
		payload[2] != aggStringMetadataMagic2 {
		return payload, types.RuntimeStringInherit, types.StringSourceExpression, false, nil
	}
	if payload[3] != aggStringMetadataVersion {
		return nil, types.RuntimeStringInherit, types.StringSourceExpression, false,
			moerr.NewInvalidInputNoCtx("invalid aggregate string metadata version")
	}
	domain = types.RuntimeStringDomain(payload[4])
	source = types.StringSource(payload[5])
	if !domain.Valid() || !source.Valid() {
		return nil, types.RuntimeStringInherit, types.StringSourceExpression, false,
			moerr.NewInvalidInputNoCtx("invalid aggregate string metadata")
	}
	return payload[aggStringMetadataHeaderSize:], domain, source, true, nil
}

func (ag *aggState) fillDistinctArgInInputOrder(
	mp *mpool.MPool,
	y uint16,
	key []byte,
) error {
	var ordinal [kAggArgOrdinalSz]byte
	binary.BigEndian.PutUint32(ordinal[:], ag.argCnt[y])
	err := ag.insertArgValue(mp, key, ordinal[:])
	if err == arenaskl.ErrRecordExists {
		return nil
	}
	if err != nil {
		return err
	}
	ag.argCnt[y]++
	if ag.argCnt[y] == 0 {
		return moerr.NewInternalErrorNoCtx(
			"agg fillArg: too many distinct arguments")
	}
	return nil
}

func (ag *aggState) insertPreparedArg(
	mp *mpool.MPool,
	y uint16,
	key []byte,
	distinct bool,
) error {
	err := ag.insertArg(mp, key)
	if err == arenaskl.ErrRecordExists && distinct {
		return nil
	}
	if err != nil {
		return err
	}
	ag.argCnt[y]++
	if ag.argCnt[y] == 0 {
		if distinct {
			return moerr.NewInternalErrorNoCtx(
				"agg fillArg: too many distinct arguments")
		}
		return moerr.NewInternalErrorNoCtx("agg fillArg: too many arguments")
	}
	return nil
}

func (ag *aggState) mergeArgs(mp *mpool.MPool, y uint16, other *aggState, otherY uint16, info *aggInfo) error {
	if info.isDistinct && ag.distinctFixedDeferred && ag.distinctKeyWidth > 0 {
		if other == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if other.distinctFixedDeferred &&
			ag.distinctKeyWidth == other.distinctKeyWidth {
			return ag.mergeFixedDistinctArgs(mp, y, other, otherY)
		}
		// A fixed target can receive a legacy source when the source was
		// created under a smaller account or restored from the compatibility
		// skiplist representation.  Keep the target's representation stable and
		// import the source's fixed-width payloads directly into its index.
		return ag.mergeLegacyDistinctArgsIntoFixed(mp, y, other, otherY)
	}
	var inserter arenaskl.Inserter
	merge := func(k []byte) error {
		kcpy, err := ag.resizeArgScratch(mp, len(k))
		if err != nil {
			return err
		}
		copy(kcpy, k)
		binary.BigEndian.PutUint16(kcpy[:kAggArgPrefixSz], y)
		if !info.isDistinct {
			binary.BigEndian.PutUint32(kcpy[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz], ag.argCnt[y])
		}
		if info.preserveDistinctInputOrder {
			return ag.fillDistinctArgInInputOrder(mp, y, kcpy)
		}
		fnerr := ag.insertArgValueWithInserter(mp, kcpy, nil, &inserter)
		if fnerr == nil {
			ag.argCnt[y] += 1
			if ag.argCnt[y] == 0 {
				return moerr.NewInternalErrorNoCtx("agg mergeArgs: too many arguments")
			}
			return nil
		} else if fnerr == arenaskl.ErrRecordExists {
			if info.isDistinct {
				return nil
			}
			return moerr.NewInternalErrorNoCtx(
				"agg mergeArgs: duplicate arguments")
		} else {
			return fnerr
		}
	}
	if info.preserveDistinctInputOrder {
		return other.iterInputOrder(mp, otherY, merge)
	}
	return other.iter(otherY, merge)
}

// mergeFixedDistinctArgs imports a source fixed index without rebuilding the
// compatibility key or re-entering the skiplist wrapper for every value.
func (ag *aggState) mergeFixedDistinctArgs(
	mp *mpool.MPool,
	y uint16,
	other *aggState,
	otherY uint16,
) error {
	if y >= uint16(len(ag.argCnt)) || otherY >= uint16(len(other.argCnt)) ||
		other.argCnt[otherY] > math.MaxUint32-ag.argCnt[y] {
		return moerr.NewInternalErrorNoCtx(
			"agg mergeArgs: too many distinct arguments")
	}
	added, err := ag.distinctIndex.mergeInto(
		mp, ag.allocation, y, &other.distinctIndex, otherY)
	if added > math.MaxUint32-ag.argCnt[y] {
		return moerr.NewInternalErrorNoCtx(
			"agg mergeArgs: too many distinct arguments")
	}
	ag.argCnt[y] += added
	if err != nil {
		return err
	}
	return nil
}

// mergeLegacyDistinctArgsIntoFixed converts the source's compatibility
// skiplist keys back to their fixed-width payload and publishes them directly
// into the target index.  Preflight reserves an upper bound based on the source
// argument count, so valid inputs do not allocate at publication time.
func (ag *aggState) mergeLegacyDistinctArgsIntoFixed(
	mp *mpool.MPool,
	y uint16,
	other *aggState,
	otherY uint16,
) error {
	if ag == nil || other == nil || ag.distinctKeyWidth <= 0 ||
		other.argSkl == nil ||
		y >= uint16(len(ag.argCnt)) || otherY >= uint16(len(other.argCnt)) ||
		other.argCnt[otherY] > math.MaxUint32-ag.argCnt[y] {
		return moerr.NewInternalErrorNoCtx(
			"agg mergeArgs: too many distinct arguments")
	}
	var added uint32
	err := other.iter(otherY, func(key []byte) error {
		_, value, ok := decodeDistinctFixedKey(key, ag.distinctKeyWidth)
		if !ok {
			return mpool.ErrAllocationAccountInvariant
		}
		duplicate, err := ag.distinctIndex.prepare(
			mp, ag.allocation, y, value)
		if err != nil {
			return err
		}
		if duplicate {
			return nil
		}
		if err := ag.distinctIndex.insert(y, value); err != nil {
			return err
		}
		added++
		return nil
	})
	if added > math.MaxUint32-ag.argCnt[y] {
		return moerr.NewInternalErrorNoCtx(
			"agg mergeArgs: too many distinct arguments")
	}
	ag.argCnt[y] += added
	return err
}

func (ag *aggState) iter(idx uint16, fn func(k []byte) error) error {
	if ag.distinctFixedDeferred {
		if int(idx) >= len(ag.argCnt) {
			return mpool.ErrAllocationAccountInvariant
		}
		// Empty fixed-index rows are valid before the first key is admitted.
		// Their optional index columns are intentionally unallocated; callers
		// such as spill drains and generic merge fallback still iterate every
		// physical group row and must observe an empty stream, not an error.
		if ag.argCnt[idx] == 0 {
			return nil
		}
		if len(ag.distinctIndex.groupHead) == 0 {
			return mpool.ErrAllocationAccountInvariant
		}
		width := ag.distinctKeyWidth
		var encoded [kAggArgPrefixSz + 8]byte
		binary.BigEndian.PutUint16(encoded[:kAggArgPrefixSz], idx)
		return ag.distinctIndex.forEach(idx, func(value uint64) error {
			binary.LittleEndian.PutUint64(encoded[kAggArgPrefixSz:], value)
			return fn(encoded[:kAggArgPrefixSz+width])
		})
	}
	var lkb, ukb [kAggArgPrefixSz]byte
	lk := lkb[:]
	uk := ukb[:]
	binary.BigEndian.PutUint16(lk, idx)
	binary.BigEndian.PutUint16(uk, idx+1)
	it := ag.argSkl.NewIter(lk, uk)
	defer it.Close()
	for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

type orderedDistinctArgument struct {
	key []byte
}

func (ag *aggState) iterInputOrder(
	mp *mpool.MPool,
	idx uint16,
	fn func(k []byte) error,
) error {
	count := int(ag.argCnt[idx])
	entries, err := makeAccountedScratch[orderedDistinctArgument](
		ag.allocation, mp, count)
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(mp, entries)

	var lkb, ukb [kAggArgPrefixSz]byte
	lk, uk := lkb[:], ukb[:]
	binary.BigEndian.PutUint16(lk, idx)
	binary.BigEndian.PutUint16(uk, idx+1)
	it := ag.argSkl.NewIter(lk, uk)
	defer it.Close()
	seen := 0
	for ok, key, value := it.SeekGE(lk); ok; ok, key, value = it.Next() {
		if len(value) != kAggArgOrdinalSz {
			return mpool.ErrAllocationAccountInvariant
		}
		ordinal := int(binary.BigEndian.Uint32(value))
		if ordinal < 0 || ordinal >= count || entries[ordinal].key != nil {
			return mpool.ErrAllocationAccountInvariant
		}
		entries[ordinal].key = key
		seen++
	}
	if seen != count {
		return mpool.ErrAllocationAccountInvariant
	}
	for i := range entries {
		if entries[i].key == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := fn(entries[i].key); err != nil {
			return err
		}
	}
	return nil
}

func aggPayloadOffset(info *aggInfo) int {
	if info.isDistinct {
		return kAggArgPrefixSz
	}
	return kAggArgPrefixSz + kAggArgOrdinalSz
}

func aggPayloadFromKey(info *aggInfo, k []byte) []byte {
	return k[aggPayloadOffset(info):]
}

func (ag *aggState) free(mp *mpool.MPool) {
	ag.distinctIndex.free(mp)
	ag.distinctKeyWidth = 0
	ag.distinctFixedDeferred = false
	if cap(ag.argCnt) > 0 {
		mpool.FreeSlice(mp, ag.argCnt)
	}
	ag.argCnt = nil
	if cap(ag.argbuf) > 0 {
		mp.Free(ag.argbuf)
	}
	ag.argbuf = nil
	ag.argSkl = nil
	if cap(ag.argScratch) > 0 {
		mp.Free(ag.argScratch)
	}
	ag.argScratch = nil
	ag.boundedArgumentGrowth = false
	for _, vec := range ag.vecs {
		vec.Free(mp)
	}
	ag.vecs = nil
	for _, mob := range ag.mobs {
		if freeable, ok := mob.(freeableMarshalerUnmarshaler); ok {
			freeable.Free()
		}
	}
	ag.mobs = nil
	ag.length = 0
	ag.capacity = 0
	ag.allocation = nil
}

type aggExec struct {
	mp *mpool.MPool
	aggInfo
	chunkSize              int
	state                  []aggState
	standby                []aggState
	allocation             *AllocationAccount
	distinctFixedAdmission distinctFixedAdmissionPlan
}

func (ae *aggExec) finalizeStringSourcePreflights(groups []uint64) {
	if ae == nil {
		return
	}
	// Only vectors touched by this work unit can carry a retained preflight.
	// Finalize is idempotent after the first group in a chunk, so duplicate
	// groups remain O(1) without scanning all historical aggregate state.
	for _, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, _ := ae.getXY(group - 1)
		if x < 0 || x >= len(ae.state) {
			continue
		}
		for _, vec := range ae.state[x].vecs {
			vec.FinalizeStringSourcePreflight()
		}
	}
}

func (ae *aggExec) SetAllocationAccount(allocation *AllocationAccount) error {
	if ae == nil || allocation == nil || allocation.account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ae.allocation != nil {
		if ae.allocation.sameGeneration(allocation) {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if len(ae.state) != 0 || len(ae.standby) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	if ae.aggInfo.makeMarshalerUnmarshaler != nil && !ae.aggInfo.boundedOpaqueState {
		return moerr.NewNotSupportedNoCtxf(
			"aggregate %d has opaque allocation state", ae.aggInfo.aggId)
	}
	ae.allocation = allocation
	return nil
}

func (ae *aggExec) ClearAllocationAccount(allocation *AllocationAccount) error {
	if ae == nil || ae.allocation == nil {
		return nil
	}
	if !ae.allocation.sameGeneration(allocation) {
		return mpool.ErrAllocationAccountMismatch
	}
	if len(ae.state) != 0 || len(ae.standby) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	ae.allocation = nil
	return nil
}

// SetPrepareParamKind restores the scalar compatibility summary on a
// deserialized preserving aggregate state. Row-exact metadata, when present,
// is installed by the aggregate implementation and remains authoritative;
// this method is only the v1 partial-state fallback.
func (ae *aggExec) SetPrepareParamKind(kind vector.PrepareParamKind) {
	for i := range ae.state {
		if len(ae.state[i].vecs) > 0 && ae.state[i].vecs[0] != nil {
			// An exact winner sidecar is authoritative. A scalar trailer can
			// restore only legacy states that did not carry row provenance.
			if len(ae.state[i].vecs[0].GetPrepareParamKinds()) == 0 {
				ae.state[i].vecs[0].SetPrepareParamKind(kind)
			}
		}
	}
}

func (ae *aggExec) PrepareParamKindChunkCount() int {
	if ae == nil {
		return 0
	}
	return len(ae.state)
}

func (ae *aggExec) PrepareParamKindVectorForChunk(chunk int) *vector.Vector {
	if ae == nil || chunk < 0 || chunk >= len(ae.state) ||
		len(ae.state[chunk].vecs) == 0 {
		return nil
	}
	return ae.state[chunk].vecs[0]
}

func (ae *aggExec) getChunkSize() int {
	return ae.chunkSize
}

func (ae *aggExec) modifyChunkSize(n int) {
	if n != 1 && n != AggBatchSize {
		panic(moerr.NewInternalErrorNoCtxf("invalid chunk size: %d", n))
	}
	ae.chunkSize = n
}

func (ae *aggExec) GetOptResult() SplitResult {
	return ae
}

func (ae *aggExec) getXY(u uint64) (int, uint16) {
	return int(u >> aggBatchSizeShift), uint16(u & aggBatchSizeMask)
}

func chunkArr[T any](v *vector.Vector) *[AggBatchSize]T {
	return (*[AggBatchSize]T)(vector.MustFixedColAsSlice[T](v, AggBatchSize))
}

// chunkRows returns only the logical state rows. Spill decode deliberately
// gives source chunks exact capacity, so merge paths must not impose the
// destination's fixed-capacity array contract on their immutable source.
func chunkRows[T any](v *vector.Vector) []T {
	return vector.MustFixedColNoTypeCheck[T](v)
}

func (ae *aggExec) GetNumChunks() int {
	return len(ae.state)
}

func (ae *aggExec) GetNumGroups() int {
	num := 0
	for _, state := range ae.state {
		num += int(state.length)
	}
	return num
}

func (*aggExec) AdditionalMemorySize() int64 { return 0 }

func (ae *aggExec) GroupGrow(more int) error {
	if ae.chunkSize == 1 {
		ae.state = make([]aggState, 1)
		if err := ae.state[0].initWithAllocation(ae.mp, 0, 1, &ae.aggInfo, true, ae.allocation); err != nil {
			ae.state = nil
			return err
		}
		// Ensure vecs have AggBatchSize capacity so chunkArr is safe.
		for _, vec := range ae.state[0].vecs {
			if vec != nil && vec.Capacity() < AggBatchSize {
				if err := vec.PreExtend(AggBatchSize, ae.mp); err != nil {
					ae.state[0].free(ae.mp)
					ae.state = nil
					return err
				}
			}
		}
		if _, _, err := ae.state[0].grow(ae.mp, 1, true); err != nil {
			ae.state[0].free(ae.mp)
			ae.state = nil
			return err
		}
		return nil
	}

	// grow the state until the more groups are added
	for remain := int32(more); remain > 0; {
		if len(ae.state) != 0 {
			var err error
			_, remain, err = ae.state[len(ae.state)-1].grow(ae.mp, remain, true)
			if err != nil {
				return err
			}
		}

		if remain == 0 {
			return nil
		}
		if len(ae.state) == 0 && len(ae.standby) != 0 {
			// The first preallocated chunk can become the active zero-length
			// chunk without allocating a second slice. Cap the view so later
			// active appends cannot overwrite the remaining standby chunks.
			ae.state = ae.standby[:1:1]
			ae.standby = ae.standby[1:]
			continue
		}
		var next aggState
		if len(ae.standby) != 0 {
			next = ae.standby[0]
			ae.standby[0] = aggState{}
			ae.standby = ae.standby[1:]
		} else if err := next.initWithAllocation(
			ae.mp, 0, AggBatchSize, &ae.aggInfo, true, ae.allocation,
		); err != nil {
			return err
		}
		ae.state = append(ae.state, next)
	}
	return nil
}

func (ae *aggExec) preAllocateGroupsWithNulls(more int, setNulls bool) error {
	if more < 0 {
		return moerr.NewInternalErrorNoCtxf("invalid more: %d", more)
	}

	// grow the state until the more groups are added
	for remain := int32(more); remain > 0; {
		if len(ae.state) != 0 {
			_, remain, _ = ae.state[len(ae.state)-1].grow(ae.mp, remain, false)
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState{})
		if err := ae.state[len(ae.state)-1].initWithAllocation(ae.mp, 0, AggBatchSize, &ae.aggInfo, setNulls, ae.allocation); err != nil {
			ae.state = ae.state[:len(ae.state)-1]
			return err
		}
	}
	return nil
}

func (ae *aggExec) PreAllocateGroups(more int) error {
	if more < 0 {
		return moerr.NewInternalErrorNoCtxf("invalid more: %d", more)
	}
	if more == 0 || ae.chunkSize == 1 {
		return nil
	}

	available := 0
	if len(ae.state) != 0 {
		last := &ae.state[len(ae.state)-1]
		available = int(last.capacity - last.length)
	}
	for i := range ae.standby {
		available += int(ae.standby[i].capacity)
	}
	for available < more {
		var next aggState
		if err := next.initWithAllocation(
			ae.mp, 0, AggBatchSize, &ae.aggInfo, true, ae.allocation,
		); err != nil {
			return err
		}
		ae.standby = append(ae.standby, next)
		available += int(next.capacity)
	}
	return nil
}

// Fill, BulkFill, BatchFill, and Flush are implemented by each agg function.
// SetExtraInformation also implemented by each agg.

func (ae *aggExec) SaveIntermediateResult(cnt int64, flags [][]uint8, writer io.Writer) error {
	return ae.SaveIntermediateResultWithStringSource(cnt, flags, writer, true)
}

func (ae *aggExec) SaveIntermediateResultWithStringSource(
	cnt int64,
	flags [][]uint8,
	writer io.Writer,
	includeStringSource bool,
) error {
	magic := magicNumber
	if err := types.WriteUint64(writer, magic); err != nil {
		return err
	}

	// Empty chunks carry no positional meaning in the intermediate format: the
	// reader packs every selected state contiguously. Group spill commonly
	// selects one 8K state chunk out of hundreds, so emitting all of the empty
	// chunk headers bloats millions of small records and adds needless decode work.
	// A nil/empty flag slice is the caller's compact representation of an empty
	// chunk; non-empty all-zero flags remain encoded for compatibility.
	var chunks int32
	for i := range flags {
		if len(flags[i]) > 0 {
			chunks++
		}
	}
	if err := types.WriteInt32(writer, chunks); err != nil {
		return err
	}
	for i := range flags {
		if len(flags[i]) == 0 {
			continue
		}
		if i >= len(ae.state) {
			return moerr.NewInternalErrorNoCtxf("aggregate state chunk out of range: %d >= %d", i, len(ae.state))
		}
		if err := ae.state[i].writeStateToBuf(ae.mp, &ae.aggInfo, flags[i], writer); err != nil {
			return err
		}
	}
	if err := ae.writeBinaryStringTrailerForSelection(flags, writer, includeStringSource); err != nil {
		return err
	}

	if err := types.WriteUint64(writer, magic); err != nil {
		return err
	}
	return nil
}

func (ae *aggExec) SaveSpillIntermediateRows(
	chunk int,
	rows []int32,
	writer io.Writer,
) error {
	if ae == nil || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate spill state")
	}
	if chunk < 0 || chunk >= len(ae.state) || len(rows) == 0 {
		return moerr.NewInternalErrorNoCtx("aggregate spill state chunk is invalid")
	}
	if err := types.WriteUint64(writer, spillMagicNumber); err != nil {
		return err
	}
	written, err := ae.state[chunk].writeSpillStateRows(
		ae.mp, &ae.aggInfo, rows, writer)
	if err != nil {
		return err
	}
	if int(written) != len(rows) {
		return moerr.NewInternalErrorNoCtxf(
			"aggregate spill count %d does not match %d", written, len(rows))
	}
	return types.WriteUint64(writer, spillMagicNumber)
}

func (ae *aggExec) UnmarshalSpillFromReader(
	reader io.Reader,
	mp *mpool.MPool,
) (retErr error) {
	if ae == nil || reader == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate spill decoder")
	}
	magic, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if magic != spillMagicNumber {
		return moerr.NewInvalidInputNoCtx("invalid aggregate spill header")
	}
	defer func() {
		if retErr != nil {
			ae.Free()
		}
	}()
	ae.freeStandby()
	if len(ae.state) == 0 {
		ae.state = make([]aggState, 1)
	} else if len(ae.state) != 1 {
		ae.Free()
		ae.state = make([]aggState, 1)
	}
	if _, err = ae.state[0].readSpillState(
		mp, reader, &ae.aggInfo, ae.allocation); err != nil {
		return err
	}
	magic, err = types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if magic != spillMagicNumber {
		return moerr.NewInvalidInputNoCtx("invalid aggregate spill trailer")
	}
	return nil
}

func (ae *aggExec) SaveIntermediateResultOfChunk(chunk int, writer io.Writer) error {
	return ae.SaveIntermediateResultOfChunkWithStringSource(chunk, writer, true)
}

func (ae *aggExec) SaveIntermediateResultOfChunkWithStringSource(
	chunk int,
	writer io.Writer,
	includeStringSource bool,
) error {
	if chunk < 0 || chunk >= len(ae.state) {
		return moerr.NewInternalErrorNoCtx("chunk index out of range")
	}

	magic := magicNumber
	if err := types.WriteUint64(writer, magic); err != nil {
		return err
	}

	if err := types.WriteInt32(writer, int32(1)); err != nil {
		return err
	}
	if err := ae.state[chunk].writeAllStatesToBuf(
		ae.mp, writer, &ae.aggInfo); err != nil {
		return err
	}
	if err := ae.writeBinaryStringTrailerForChunk(chunk, writer, includeStringSource); err != nil {
		return err
	}

	if err := types.WriteUint64(writer, magic); err != nil {
		return err
	}

	return nil
}

func writeAggBinaryStringByte(writer io.Writer, value byte) error {
	var encoded [1]byte
	encoded[0] = value
	written, err := writer.Write(encoded[:])
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}

func (ae *aggExec) writeBinaryStringTrailerForSelection(
	flags [][]uint8,
	writer io.Writer,
	includeStringSource bool,
) error {
	hasBinaryString := false
	hasTextString := false
	hasStringSource := false
	var rowCount int32
	for chunk, chunkFlags := range flags {
		if chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row, flag := range chunkFlags {
			if flag == 0 {
				continue
			}
			rowCount++
			if vec != nil && vec.GetRuntimeStringDomainAt(row) != types.RuntimeStringInherit {
				hasBinaryString = true
				hasTextString = hasTextString || vec.GetRuntimeStringDomainAt(row) == types.RuntimeStringText
			}
			hasStringSource = includeStringSource && (hasStringSource ||
				(vec != nil && vec.GetStringSourceAt(row) != types.StringSourceExpression))
		}
	}
	if !hasBinaryString && !hasStringSource {
		return nil
	}
	marker := aggBinaryStringTrailerMagic
	if hasTextString {
		marker = aggStringDomainTrailerMagic
	}
	if hasStringSource {
		marker = aggStringStateTrailerMagic
	}
	if err := types.WriteUint64(writer, marker); err != nil {
		return err
	}
	if err := types.WriteInt32(writer, rowCount); err != nil {
		return err
	}
	for chunk, chunkFlags := range flags {
		if chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row, flag := range chunkFlags {
			if flag == 0 {
				continue
			}
			value := byte(0)
			if vec != nil {
				if marker == aggStringStateTrailerMagic {
					value = byte(vec.GetRuntimeStringDomainAt(row)) |
						byte(vec.GetStringSourceAt(row))<<2
				} else if hasTextString {
					value = byte(vec.GetRuntimeStringDomainAt(row))
				} else if vec.GetRuntimeStringDomainAt(row) == types.RuntimeStringBinary {
					value = 1
				}
			}
			if err := writeAggBinaryStringByte(writer, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ae *aggExec) writeBinaryStringTrailerForChunk(
	chunk int,
	writer io.Writer,
	includeStringSource bool,
) error {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 ||
		ae.state[chunk].vecs[0] == nil ||
		(!ae.state[chunk].vecs[0].HasBinaryStringMetadata() &&
			(!includeStringSource || !ae.state[chunk].vecs[0].HasStringSourceMetadata())) {
		return nil
	}
	vec := ae.state[chunk].vecs[0]
	marker := aggBinaryStringTrailerMagic
	if vec.HasExplicitTextStringMetadata() {
		marker = aggStringDomainTrailerMagic
	}
	if includeStringSource && vec.HasStringSourceMetadata() {
		marker = aggStringStateTrailerMagic
	}
	if err := types.WriteUint64(writer, marker); err != nil {
		return err
	}
	if err := types.WriteInt32(writer, int32(vec.Length())); err != nil {
		return err
	}
	for row := 0; row < vec.Length(); row++ {
		value := byte(0)
		if marker == aggStringStateTrailerMagic {
			value = byte(vec.GetRuntimeStringDomainAt(row)) | byte(vec.GetStringSourceAt(row))<<2
		} else if marker == aggStringDomainTrailerMagic {
			value = byte(vec.GetRuntimeStringDomainAt(row))
		} else if vec.GetRuntimeStringDomainAt(row) == types.RuntimeStringBinary {
			value = 1
		}
		if err := writeAggBinaryStringByte(writer, value); err != nil {
			return err
		}
	}
	return nil
}

func checkAggStateMagic(reader io.Reader) error {
	magic, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if magic != magicNumber {
		return moerr.NewInvalidInputNoCtxf(
			"invalid aggregate state magic number %d", magic)
	}
	return nil
}

func (ae *aggExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) (retErr error) {
	if ae == nil || reader == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate state decoder")
	}
	defer func() {
		if retErr != nil {
			ae.Free()
			ae.state = nil
		}
	}()
	if err := checkAggStateMagic(reader); err != nil {
		return err
	}
	ae.freeStandby()

	// read number of chunks
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if cnt < 0 {
		return moerr.NewInvalidInputNoCtxf(
			"invalid aggregate state chunk count %d", cnt)
	}

	// nothing to read
	if cnt == 0 {
		ae.Free()
		ae.state = nil
		return ae.readBinaryStringTrailerAndMagic(reader, mp)
	} else if cnt == 1 {
		// The compact spill format makes this the common path. Retain one simple
		// fixed-state chunk across records so UnmarshalWithReader can reuse its
		// off-heap vector capacity instead of mmap/munmap for every small record.
		if len(ae.state) != 1 {
			ae.Free()
			ae.state = make([]aggState, 1)
		}
		if _, err := ae.state[0].readStateWithAllocation(mp, reader, &ae.aggInfo, ae.allocation); err != nil {
			return err
		}
		// Ensure vecs have AggBatchSize capacity so chunkArr is safe.
		for _, vec := range ae.state[0].vecs {
			if vec != nil && vec.Capacity() < AggBatchSize {
				if err := vec.PreExtend(AggBatchSize-vec.Length(), mp); err != nil {
					return err
				}
			}
		}
		if !ae.aggInfo.saveArg && ae.aggInfo.makeMarshalerUnmarshaler == nil {
			ae.state[0].capacity = AggBatchSize
		}
		return ae.readBinaryStringTrailerAndMagic(reader, mp)
	}

	// Multi-chunk inputs may need to repack several independently allocated
	// states. Keep their historical cleanup path; only the one-chunk path above
	// has a complete bounded reuse invariant.
	ae.Free()
	ae.state = nil

	// multi chunks to read, in this case, we will read each chunk and merge them
	// into fully packed chunks.
	for range cnt {
		err = func() error {
			var st aggState
			defer st.free(mp)
			if _, err := st.readStateWithAllocation(mp, reader, &ae.aggInfo, ae.allocation); err != nil {
				return err
			}
			if st.length == 0 {
				return nil
			}

			oldX := max(0, len(ae.state)-1)
			if err := ae.preAllocateGroupsWithNulls(int(st.length), false); err != nil {
				return err
			}
			offset, err := ae.state[oldX].appendFromStateArg(mp, 0, &st, &ae.aggInfo)
			if err != nil {
				return err
			}
			if offset < st.length {
				oldX += 1
				offset, err = ae.state[oldX].appendFromStateArg(mp, offset, &st, &ae.aggInfo)
				// we should not have any remaining
				if err != nil || offset != st.length {
					return moerr.NewInternalErrorNoCtxf("invalid read count: %d", offset)
				}
			}
			return nil
		}()

		if err != nil {
			return err
		}
	}
	return ae.readBinaryStringTrailerAndMagic(reader, mp)
}

func (ae *aggExec) readBinaryStringTrailerAndMagic(reader io.Reader, mp *mpool.MPool) error {
	marker, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if marker == magicNumber {
		return nil
	}
	if marker != aggBinaryStringTrailerMagic && marker != aggStringDomainTrailerMagic &&
		marker != aggStringStateTrailerMagic {
		return moerr.NewInvalidInputNoCtxf(
			"invalid aggregate state magic number %d", marker)
	}
	rowCount, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if rowCount < 0 || int(rowCount) != ae.GetNumGroups() {
		return moerr.NewInvalidInputNoCtxf(
			"aggregate binary provenance row count %d does not match %d", rowCount, ae.GetNumGroups())
	}
	for chunk := range ae.state {
		if len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row := 0; row < vec.Length(); row++ {
			encoded, err := types.ReadByte(reader)
			if err != nil {
				return err
			}
			domain := encoded
			source := types.StringSourceExpression
			if marker == aggStringStateTrailerMagic {
				domain = encoded & 0x03
				source = types.StringSource(encoded >> 2)
			}
			if marker == aggBinaryStringTrailerMagic && domain > 1 ||
				(marker == aggStringDomainTrailerMagic || marker == aggStringStateTrailerMagic) &&
					types.RuntimeStringDomain(domain) > types.RuntimeStringBinary || !source.Valid() {
				return moerr.NewInvalidInputNoCtx("invalid aggregate binary provenance row")
			}
			runtimeDomain := types.RuntimeStringDomain(domain)
			if marker == aggBinaryStringTrailerMagic && domain == 1 {
				runtimeDomain = types.RuntimeStringBinary
			}
			if err := vec.SetRuntimeStringDomainAtWithMP(row, runtimeDomain, mp); err != nil {
				return err
			}
			if err := vec.SetStringSourceAtWithMP(row, source, mp); err != nil {
				return err
			}
		}
	}
	return checkAggStateMagic(reader)
}

func (ae *aggExec) Size() int64 {
	panic("not implemented")
}

func (ae *aggExec) Free() {
	ae.distinctFixedAdmission.reset()
	for _, st := range ae.state {
		st.free(ae.mp)
	}
	ae.state = nil
	ae.freeStandby()
}

func (ae *aggExec) freeStandby() {
	for _, st := range ae.standby {
		st.free(ae.mp)
	}
	ae.standby = nil
}

// canonicalDistinctArgumentSize reports the fixed-width key size for a signed
// zero. The caller writes the canonical all-zero payload into its existing
// accounted scratch buffer, avoiding a temporary slice and heap allocation.
func canonicalDistinctArgumentSize(vec *vector.Vector, row int) (int, bool) {
	switch vec.GetType().Oid {
	case types.T_float32:
		if vector.MustFixedColNoTypeCheck[float32](vec)[row] == 0 {
			return 4, true
		}
	case types.T_float64:
		if vector.MustFixedColNoTypeCheck[float64](vec)[row] == 0 {
			return 8, true
		}
	}
	return 0, false
}

// copyCanonicalDistinctArgument copies one DISTINCT payload into dst. Signed
// zero is represented by the native-endian all-zero payload used by the
// resident aggregate key, while all other values retain their raw bytes.
func copyCanonicalDistinctArgument(dst []byte, vec *vector.Vector, row int) int {
	if size, ok := canonicalDistinctArgumentSize(vec, row); ok {
		clear(dst[:size])
		return size
	}
	return copy(dst, vec.GetRawBytesAt(row))
}

func distinctFixedValue(vec *vector.Vector, row, width int) (uint64, error) {
	if vec == nil || row < 0 || width <= 0 || width > 8 {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	if size, ok := canonicalDistinctArgumentSize(vec, row); ok {
		if size != width {
			return 0, mpool.ErrAllocationAccountInvariant
		}
		return 0, nil
	}
	raw := vec.GetRawBytesAt(row)
	if len(raw) != width {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	var padded [8]byte
	copy(padded[:], raw)
	return binary.LittleEndian.Uint64(padded[:]), nil
}

func distinctArgumentRowsEqual(vec *vector.Vector, left, right int) bool {
	leftZero := false
	if _, ok := canonicalDistinctArgumentSize(vec, left); ok {
		leftZero = true
	}
	rightZero := false
	if _, ok := canonicalDistinctArgumentSize(vec, right); ok {
		rightZero = true
	}
	if leftZero || rightZero {
		return leftZero && rightZero
	}
	return bytes.Equal(vec.GetRawBytesAt(left), vec.GetRawBytesAt(right))
}

func (ag *aggState) fillDistinctVectorArg(
	mp *mpool.MPool,
	y uint16,
	vec *vector.Vector,
	row int,
) error {
	val := vec.GetRawBytesAt(row)
	if size, ok := canonicalDistinctArgumentSize(vec, row); ok {
		val = val[:size]
	}
	k, err := ag.resizeArgScratch(mp, kAggArgPrefixSz+len(val))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(k[:kAggArgPrefixSz], y)
	copyCanonicalDistinctArgument(k[kAggArgPrefixSz:], vec, row)
	return ag.insertPreparedArg(mp, y, k, true)
}

func (ae *aggExec) fixedDistinctBatchWidth(groups []uint64, vec *vector.Vector) int {
	if ae == nil || vec == nil || !ae.isDistinct || !ae.saveArg {
		return 0
	}
	width := distinctFixedKeyWidth(&ae.aggInfo)
	if width == 0 {
		return 0
	}
	for _, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y := ae.getXY(group - 1)
		if x < 0 || x >= len(ae.state) {
			return 0
		}
		state := &ae.state[x]
		if int(y) >= int(state.capacity) || !state.distinctFixedDeferred ||
			state.distinctKeyWidth != width {
			return 0
		}
	}
	return width
}

func (ae *aggExec) batchFillFixedDistinctArgs(
	offset int,
	groups []uint64,
	vec *vector.Vector,
	width int,
) error {
	if ae.distinctFixedAdmission.matches(offset, groups, vec) {
		plan := &ae.distinctFixedAdmission
		plan.valid = false
		for i, group := range groups {
			if !plan.publish[i] {
				continue
			}
			x, y := ae.getXY(group - 1)
			if x < 0 || x >= len(ae.state) {
				return mpool.ErrAllocationAccountInvariant
			}
			state := &ae.state[x]
			if state.distinctKeyWidth != width ||
				int(y) >= int(state.capacity) {
				return mpool.ErrAllocationAccountInvariant
			}
			if state.argCnt[y] == math.MaxUint32 {
				return moerr.NewInternalErrorNoCtx(
					"agg fillArg: too many distinct arguments")
			}
			if err := state.distinctIndex.insert(y, plan.values[i]); err != nil {
				return err
			}
			state.argCnt[y]++
		}
		return nil
	}
	ae.distinctFixedAdmission.reset()
	var batch distinctFixedBatch
	batch.reset()
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y := ae.getXY(group - 1)
		if x < 0 || x >= len(ae.state) {
			return mpool.ErrAllocationAccountInvariant
		}
		state := &ae.state[x]
		row, err := preflightPhysicalRow(vec, offset+i)
		if err != nil {
			return err
		}
		if vec.IsNull(uint64(row)) {
			continue
		}
		value, err := distinctFixedValue(vec, row, width)
		if err != nil {
			return err
		}
		duplicate, err := batch.seenOrInsert(group, value)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		duplicate, err = state.distinctIndex.prepare(
			ae.mp, state.allocation, y, value)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if state.argCnt[y] == math.MaxUint32 {
			return moerr.NewInternalErrorNoCtx(
				"agg fillArg: too many distinct arguments")
		}
		if err := state.distinctIndex.insert(y, value); err != nil {
			return err
		}
		state.argCnt[y]++
	}
	return nil
}

func (ae *aggExec) batchFillArgs(offset int, groups []uint64, vectors []*vector.Vector, distinct bool) error {
	// DISTINCT admission tables are bounded to one hashmap work unit.  BatchFill
	// is also used directly by BulkFill and a few aggregate tests with a full
	// vector, so preserve that API by processing large inputs as independent
	// work units.  The resident index still supplies cross-unit deduplication.
	if distinct && len(groups) > hashmap.UnitLimit {
		for start := 0; start < len(groups); start += hashmap.UnitLimit {
			end := start + hashmap.UnitLimit
			if end > len(groups) {
				end = len(groups)
			}
			if err := ae.batchFillArgs(
				offset+start, groups[start:end], vectors, distinct); err != nil {
				return err
			}
		}
		return nil
	}
	if distinct && len(vectors) == 1 {
		if width := ae.fixedDistinctBatchWidth(groups, vectors[0]); width > 0 {
			return ae.batchFillFixedDistinctArgs(
				offset, groups, vectors[0], width)
		}
	}
	// BatchFill is fed UnitLimit-sized immutable work units. DISTINCT values
	// that repeat inside one unit are guaranteed to be duplicates for the same
	// aggregate group, so avoid probing the resident skiplist for every copy.
	// Keep the tiny linear path for duplicate-heavy units and promote to the
	// bounded open-addressing table only when the unit has more representatives.
	var linear distinctArgumentLinearBatch
	var hashed distinctArgumentBatch
	hashedReady := false
	seenDistinct := func(row int) (bool, error) {
		if !distinct {
			return false, nil
		}
		if hashedReady {
			return hashed.seenOrInsert(groups, vectors, offset, row)
		}
		duplicate, err := linear.seen(groups, vectors, offset, row)
		if err != nil || duplicate {
			return duplicate, err
		}
		if linear.insertUnique(row) {
			return false, nil
		}
		for i := uint16(0); i < linear.count; i++ {
			duplicate, err = hashed.seenOrInsert(
				groups, vectors, offset, int(linear.rows[i]))
			if err != nil {
				return false, err
			}
			if duplicate {
				return false, mpool.ErrAllocationAccountInvariant
			}
		}
		hashedReady = true
		return hashed.seenOrInsert(groups, vectors, offset, row)
	}

	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		logicalRow := offset + i

		// For single-vector, use the fast path.
		if len(vectors) == 1 {
			row, err := preflightPhysicalRow(vectors[0], logicalRow)
			if err != nil {
				return err
			}
			if vectors[0].IsNull(uint64(row)) {
				continue
			}
			duplicate, err := seenDistinct(i)
			if err != nil {
				return err
			}
			if duplicate {
				continue
			}
			x, y := ae.getXY(group - 1)
			if distinct {
				if err := ae.state[x].fillDistinctVectorArg(ae.mp, y, vectors[0], row); err != nil {
					return err
				}
				continue
			}
			bs := vectors[0].GetRawBytesAt(row)
			var fillErr error
			if ae.preserveStringMetadata && vectors[0].GetType().Oid.IsMySQLString() {
				domain := vectors[0].GetRuntimeStringDomainAt(row)
				source := vectors[0].GetStringSourceAt(row)
				if domain != types.RuntimeStringInherit || source != types.StringSourceExpression {
					fillErr = ae.state[x].fillArgWithStringMetadata(
						ae.mp, y, bs, domain, source, distinct)
				} else {
					fillErr = ae.state[x].fillArg(ae.mp, y, bs, distinct)
				}
			} else {
				fillErr = ae.state[x].fillArg(ae.mp, y, bs, distinct)
			}
			if err := fillErr; err != nil {
				return err
			}
			continue
		}

		// For multi-vector (e.g. COUNT(DISTINCT col1, col2)):
		// - Skip row if ANY column is NULL (MySQL semantics).
		// - Encode all column values into a combined key:
		//   [len1:4 bytes][raw1][len2:4 bytes][raw2]...
		hasNull := false
		for _, vec := range vectors {
			row, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			if vec.IsNull(uint64(row)) {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}
		duplicate, err := seenDistinct(i)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}

		// Calculate total encoded size without retaining a row-frequency Go
		// slice of column payloads.
		totalSize := 0
		for _, vec := range vectors {
			row, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			raw := vec.GetRawBytesAt(row)
			if uint64(len(raw)) > math.MaxUint32 ||
				len(raw) > math.MaxInt-totalSize-4 {
				return mpool.ErrAllocationAllocatorLimit
			}
			totalSize += 4 + len(raw)
		}

		x, y := ae.getXY(group - 1)
		state := &ae.state[x]
		header := kAggArgPrefixSz
		if !distinct {
			header += kAggArgOrdinalSz
		}
		if totalSize > math.MaxInt-header {
			return mpool.ErrAllocationAllocatorLimit
		}
		key, err := state.resizeArgScratch(ae.mp, header+totalSize)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], y)
		if !distinct {
			binary.BigEndian.PutUint32(
				key[kAggArgPrefixSz:header], state.argCnt[y])
		}
		off := header
		for _, vec := range vectors {
			row, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			raw := vec.GetRawBytesAt(row)
			binary.BigEndian.PutUint32(key[off:], uint32(len(raw)))
			off += 4
			if distinct {
				copyCanonicalDistinctArgument(key[off:], vec, row)
			} else {
				copy(key[off:], raw)
			}
			off += len(raw)
		}
		if err := state.insertPreparedArg(ae.mp, y, key, distinct); err != nil {
			return err
		}
	}
	return nil
}

// bulkFillDistinctArgs keeps the direct aggregate API safe for full vectors.
// The batch-local DISTINCT admission tables intentionally use one bounded
// hashmap.UnitLimit work unit; callers of BulkFill are not required to split
// their vectors before entering this layer.
func (ae *aggExec) bulkFillDistinctArgs(
	groupIndex int,
	vectors []*vector.Vector,
) error {
	if ae == nil || len(vectors) == 0 || vectors[0] == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	length := vectors[0].Length()
	var groups [hashmap.UnitLimit]uint64
	for i := range groups {
		groups[i] = uint64(groupIndex + 1)
	}
	for offset := 0; offset < length; offset += hashmap.UnitLimit {
		n := min(hashmap.UnitLimit, length-offset)
		if err := ae.batchFillArgs(offset, groups[:n], vectors, true); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) batchFillOpaqueArgs(offset int, groups []uint64, payloads [][]byte, distinct bool) error {
	_ = offset
	if len(groups) != len(payloads) {
		return moerr.NewInternalErrorNoCtx("batchFillOpaqueArgs: groups and payloads length mismatch")
	}
	for i, group := range groups {
		if group == GroupNotMatched || payloads[i] == nil {
			continue
		}
		x, y := ae.getXY(group - 1)
		if err := ae.state[x].fillArg(ae.mp, y, payloads[i], distinct); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) batchMergeArgs(next *aggExec, offset int, groups []uint64, distinct bool) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		x, y := ae.getXY(group - 1)
		otherX, otherY := next.getXY(uint64(offset + i))

		err := ae.state[x].mergeArgs(ae.mp, y, &next.state[otherX], otherY, &ae.aggInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ag *aggState) checkArgsSkl() {
	if ag.argSkl == nil {
		return
	}
	if ag.distinctFixedDeferred {
		for i := range ag.length {
			if ag.argCnt[i] == 0 {
				continue
			}
			var count uint32
			if err := ag.distinctIndex.forEach(uint16(i), func(uint64) error {
				count++
				return nil
			}); err != nil {
				panic(err)
			}
			if count != ag.argCnt[i] {
				panic(moerr.NewInternalErrorNoCtxf(
					"invalid fixed distinct count: %d for y: %d, expected: %d",
					count, i, ag.argCnt[i]))
			}
		}
		return
	}

	it := ag.argSkl.NewIter(nil, nil)
	xcnt := make([]uint32, ag.length)
	for ok, k, _ := it.First(); ok; ok, k, _ = it.Next() {
		y := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
		if y >= uint16(len(xcnt)) {
			panic(moerr.NewInternalErrorNoCtxf("invalid y: %d", y))
		}
		xcnt[y]++
	}
	it.Close()

	for i, cnt := range xcnt {
		if cnt != ag.argCnt[i] {
			panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", cnt, i, ag.argCnt[i]))
		}
	}
}

func (ae *aggExec) checkArgsSkl() {
	for _, state := range ae.state {
		state.checkArgsSkl()
	}
}
