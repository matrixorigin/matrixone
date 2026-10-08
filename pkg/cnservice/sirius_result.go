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

package cnservice

import (
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
)

func decodeEmbeddedSiriusResult(result siriusbridge.Result, request compile.SiriusPrepareRequest, mp *mpool.MPool) (bat *batch.Batch, err error) {
	if result.Rows > math.MaxInt32 || len(result.Vectors) != len(request.OutputTypes) || len(request.Headings) != len(result.Vectors) {
		return nil, moerr.NewInvalidInputNoCtx("Sirius output schema mismatch")
	}
	// Validate every external view before installing any borrowed Vector.
	var bytes uint64
	for i, v := range result.Vectors {
		t := request.OutputTypes[i]
		typ := types.New(types.T(t.Id), t.Width, t.Scale)
		if err := validateEmbeddedSiriusVector(v, typ, result.Rows, !t.NotNullable); err != nil {
			return nil, err
		}
		bytes += uint64(len(v.Data)) + uint64(len(v.Area)) + uint64(len(v.Nulls))
		if bytes > siriusbridge.WindowBytes {
			return nil, moerr.NewInvalidInputNoCtx("Sirius output exceeds native result window")
		}
	}
	if len(result.Backing) > siriusbridge.WindowBytes {
		return nil, moerr.NewInvalidInputNoCtx("Sirius output backing exceeds native result window")
	}
	accounted := bytes
	if uint64(len(result.Backing)) > accounted {
		accounted = uint64(len(result.Backing))
	}
	lease, err := vector.NewRefCountedBufferLease(result.Backing, int64(accounted), nil)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	bat = batch.NewWithSize(len(result.Vectors))
	defer func() {
		if err != nil {
			bat.Clean(mp)
			bat = nil
		}
	}()
	bat.Attrs = request.Headings
	bat.SetRowCount(int(result.Rows))
	for i, v := range result.Vectors {
		t := request.OutputTypes[i]
		typ := types.New(types.T(t.Id), t.Width, t.Scale)
		vec, createErr := vector.NewOffHeapVecWithTypeAndAllocation(typ, nil)
		if createErr != nil {
			return bat, createErr
		}
		bat.Vecs[i] = vec
		if err = vec.InstallBorrowedData(v.Data, lease); err != nil {
			return bat, err
		}
		if len(v.Area) > 0 {
			if err = vec.InstallBorrowedArea(v.Area, lease); err != nil {
				return bat, err
			}
		}
		vec.SetLength(int(result.Rows))
		// Reserve bitmap allocation while errors can still unwind this batch.
		if len(v.Nulls) != 0 {
			if err = vec.PrepareBorrowedValidity(int(result.Rows), mp); err != nil {
				return bat, err
			}
			for row := uint32(0); row < result.Rows; row++ {
				if v.Nulls[row/8]&(1<<(row%8)) != 0 {
					vec.SetNull(uint64(row))
				}
			}
		}
	}
	return bat, nil
}

func validateEmbeddedSiriusVector(v siriusbridge.Vector, typ types.Type, rows uint32, nullable bool) error {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid Sirius native vector layout") }
	width := typ.TypeSize()
	if typ.IsVarlen() {
		switch typ.Oid {
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
			width = types.VarlenaSize
		default:
			return invalid()
		}
	}
	if width <= 0 || v.Class != 0 || uint64(len(v.Data)) != uint64(rows)*uint64(width) ||
		(len(v.Nulls) != 0 && uint64(len(v.Nulls)) != (uint64(rows)+63)/64*8) || (!typ.IsVarlen() && len(v.Area) != 0) {
		return invalid()
	}
	if !nullable {
		for _, word := range v.Nulls {
			if word != 0 {
				return invalid()
			}
		}
	}
	if len(v.Nulls) > 0 && rows%64 != 0 && binary.LittleEndian.Uint64(v.Nulls[len(v.Nulls)-8:])>>(rows%64) != 0 {
		return invalid()
	}
	if typ.IsVarlen() {
		for row := uint32(0); row < rows; row++ {
			if len(v.Nulls) != 0 && v.Nulls[row/8]&(1<<(row%8)) != 0 {
				continue
			}
			d := v.Data[int(row)*types.VarlenaSize : (int(row)+1)*types.VarlenaSize]
			if binary.LittleEndian.Uint32(d) == types.VarlenaBigHdr {
				offset, length := uint64(binary.LittleEndian.Uint32(d[4:])), uint64(binary.LittleEndian.Uint32(d[8:]))
				if offset > uint64(len(v.Area)) || length > uint64(len(v.Area))-offset {
					return invalid()
				}
			} else if int(d[0]) > types.VarlenaInlineSize {
				return invalid()
			}
		}
	}
	return nil
}
