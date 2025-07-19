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

package function

/*
#include "../../../../cgo/mo.h"
*/
import "C"
import (
	"fmt"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	XCALL_L2DISTANCE_F32    = 0
	XCALL_L2DISTANCE_F64    = 1
	XCALL_L2DISTANCE_SQ_F32 = 2
	XCALL_L2DISTANCE_SQ_F64 = 3
)

type xcallArgs struct {
	ptrLens [6]uintptr
}

const xcallErrStrLen = 256

type XCallFunction struct {
	clHost      string
	clRuntimeId int64
	xFuncId     int64
	args        []xcallArgs
	ErrStr      [xcallErrStrLen]byte
}

func getRuntimeId(rt string) int64 {
	if rt == "C" {
		return 0
	} else if rt == "CUDA" {
		return 1
	} else {
		// unsppported runtime, caller handle error.
		return -1
	}
}

func c_xcall(f *XCallFunction, mp *mpool.MPool, length int, result *vector.Vector, argvecs []*vector.Vector) error {
	// allocate result vector,
	if f.args == nil {
		f.args = make([]xcallArgs, len(argvecs)+1)
	}

	result.FillRawPtrLen(f.args[0].ptrLens[0:6])
	for i, argvec := range argvecs {
		argvec.FillRawPtrLen(f.args[i+1].ptrLens[0:6])
	}

	if f.clHost == "CGO" {
		f.ErrStr[0] = 0
		cret := C.XCall(
			C.int64_t(f.clRuntimeId),
			C.int64_t(f.xFuncId),
			(*C.uint8_t)(unsafe.Pointer(&f.ErrStr[0])),
			(*C.uint64_t)(unsafe.Pointer(&f.args[0])),
			C.uint64_t(length))
		if cret != 0 {
			errLen := uint8(f.ErrStr[0])
			errStr := string(f.ErrStr[1 : 1+errLen])
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("xcall xfunc failed, error code %d, %s", cret, errStr))
		}
		return nil
	} else {
		return moerr.NewInternalErrorNoCtx("unsupported host")
	}
}

func newXCallFunction(xfId int64) *XCallFunction {
	return &XCallFunction{
		xFuncId: xfId,
	}
}

// So far XCall only support strict, not generating null, and fixed length result.
func (xcall *XCallFunction) XCall(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if xcall.clHost == "" {
		// initilize runtime info
		val, err := proc.GetResolveVariableFunc()("cl_host", true, false)
		if val == nil || err != nil {
			xcall.clHost = "CGO"
		} else {
			switch v := val.(type) {
			case string:
				xcall.clHost = v
			default:
				return moerr.NewInternalErrorNoCtx("cl_host must be string")
			}
		}

		val, err = proc.GetResolveVariableFunc()("cl_runtime", true, false)
		if val == nil || err != nil {
			// default runtime is C
			xcall.clRuntimeId = 0
		} else {
			switch v := val.(type) {
			case string:
				xcall.clRuntimeId = getRuntimeId(v)
			default:
				return moerr.NewInternalErrorNoCtx("cl_runtime must be string")
			}
		}
		if xcall.clRuntimeId == -1 {
			return moerr.NewInternalErrorNoCtx("unsupported runtime")
		}
	}

	resultVec := result.GetResultVector()
	resultVec.PreExtend(length, proc.Mp())
	rsNull := resultVec.GetNulls()

	nullsInited := false
	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsNull.InitWithSize(length)
			nullsInited = true
			for i := 0; i < length; i++ {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for _, v := range ivecs {
		if v.IsConst() {
			if v.IsConstNull() {
				nulls.AddRange(rsNull, 0, uint64(length))
				return nil
			}
		} else {
			if v.HasNull() {
				if !nullsInited {
					rsNull.InitWithSize(length)
					nullsInited = true
				}
				nulls.Or(rsNull, v.GetNulls(), rsNull)
			}
		}
	}

	return c_xcall(xcall, proc.Mp(), length, resultVec, ivecs)
}
