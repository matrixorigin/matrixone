//go:build gpu

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

package cuvs

/*
#include "../../cgo/cuvs/helper.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// DeviceReservation is a claim on a device's VRAM held in the C++ governor
// (cgo/cuvs/device_memory.hpp), the same ledger index LOADS claim through.
//
// Builds claim from Go on purpose. Go owns the per-algo cost model — CAGRA
// charges dataset+graph, IVF-PQ charges PQ codes and budgets the k-means
// trainset as max(train,index) — and restating that in C++ would fork it. A
// Go-side claim also spans the whole decided-but-not-yet-allocated window; a
// claim taken inside the C++ build would only begin at the allocation, leaving
// the minutes between planning and allocating exactly as exposed as before.
type DeviceReservation struct {
	token unsafe.Pointer
	once  sync.Once
}

// ReserveDeviceMemory claims bytes on a specific device, or returns an error
// naming what would not fit. The caller MUST Release the claim on every path;
// a leaked claim shrinks that device's budget until the process restarts.
//
// bytes == 0 is refused by the governor rather than treated as "unknown
// demand": a zero demand means the caller could not size its allocation, which
// is a defect, not permission to skip admission.
func ReserveDeviceMemory(deviceID int, bytes uint64) (*DeviceReservation, error) {
	var errmsg *C.char
	tok := C.gpu_device_memory_reserve(C.int(deviceID), C.uint64_t(bytes), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	if tok == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"cuvs: device %d refused a %d byte VRAM reservation", deviceID, bytes)
	}
	r := &DeviceReservation{token: tok}
	// Backstop only. A dropped reservation would otherwise hold VRAM budget for
	// the life of the process; the finalizer bounds that to the next GC. It is
	// NOT a substitute for Release — finalizer timing is not a resource policy.
	runtime.SetFinalizer(r, func(x *DeviceReservation) { x.Release() })
	return r, nil
}

// Release drops the claim. Idempotent and nil-safe, so `defer r.Release()` is
// safe next to an explicit call and next to a failed reservation.
func (r *DeviceReservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.token != nil {
			C.gpu_device_memory_release(r.token)
			r.token = nil
		}
		runtime.SetFinalizer(r, nil)
	})
}

// ReservedDeviceMemory reports bytes currently claimed on a device.
// For tests and diagnostics.
func ReservedDeviceMemory(deviceID int) uint64 {
	return uint64(C.gpu_device_memory_reserved(C.int(deviceID)))
}

// DeviceReservations is a set of claims across the devices one build spans.
type DeviceReservations []*DeviceReservation

// Release drops every claim in the set.
func (rs DeviceReservations) Release() {
	for _, r := range rs {
		r.Release()
	}
}

// ReserveBuildMemory claims a build's planned device bytes across the devices
// it will actually land on, and rolls the whole set back if any single device
// refuses — a partially reserved build would hold budget it can never use.
//
// perDeviceBytes maps physical device id to the bytes that device will hold.
// Callers compute it from the distribution mode: REPLICATED gives every device
// a full copy, SHARDED splits, SINGLE names one device.
func ReserveBuildMemory(perDeviceBytes map[int]uint64) (DeviceReservations, error) {
	if len(perDeviceBytes) == 0 {
		return nil, nil
	}
	out := make(DeviceReservations, 0, len(perDeviceBytes))
	for dev, want := range perDeviceBytes {
		if want == 0 {
			continue
		}
		r, err := ReserveDeviceMemory(dev, want)
		if err != nil {
			out.Release()
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
