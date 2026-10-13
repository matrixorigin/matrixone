// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spillutil

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DiskReservationWriter admits spill bytes immediately before the matching
// physical write. Partial writes reconcile the reservation to bytes that were
// actually accepted by the target.
type DiskReservationWriter struct {
	target      io.Writer
	reservation *process.ExecutionSpillDiskReservation
}

func NewDiskReservationWriter(
	target io.Writer,
	reservation *process.ExecutionSpillDiskReservation,
) *DiskReservationWriter {
	return &DiskReservationWriter{
		target:      target,
		reservation: reservation,
	}
}

func (w *DiskReservationWriter) Write(value []byte) (int, error) {
	if w == nil || w.target == nil {
		return 0, io.ErrClosedPipe
	}
	oldSize := uint64(0)
	if w.reservation != nil {
		oldSize = w.reservation.Size()
		if err := w.reservation.Grow(uint64(len(value))); err != nil {
			return 0, err
		}
	}
	written, err := w.target.Write(value)
	if written < 0 {
		written = 0
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if written > len(value) {
		written = len(value)
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if written < len(value) && w.reservation != nil {
		_, _ = w.reservation.ReconcileDown(oldSize + uint64(written))
	}
	if err == nil && written != len(value) {
		err = io.ErrShortWrite
	}
	return written, err
}
