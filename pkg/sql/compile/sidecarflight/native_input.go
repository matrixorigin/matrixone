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

package sidecarflight

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"google.golang.org/grpc"
)

var putStream = &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}

// NativeInput is one single-use, acknowledged MO-batch stream for a StreamRead.
// Send pipelines only a fixed frame/byte window. Once that window is full, a
// withheld Sirius-consumed acknowledgement blocks the native output pipeline
// and propagates backpressure to its storage readers.
type NativeInput struct {
	execution *Execution
	streamRef []byte

	mu           sync.Mutex
	stream       grpc.ClientStream
	sequence     uint64
	rows         uint64
	bytes        uint64
	pendingBytes uint64
	pending      [maxNativeInputWindowFrames]nativeInputPendingFrame
	pendingHead  int
	pendingCount int
	finished     bool
	notNeeded    bool
	terminalErr  error
	// retired is the success-valued terminal signal published by result EOF.
	// It is independent from mu so EOF can interrupt a DoPut acknowledgement
	// wait before the producer has another frame to send.
	retired atomic.Bool

	// cancelMu is intentionally independent from mu. Send and Finish hold mu
	// while waiting for a sidecar acknowledgement; Abort must be able to cancel
	// that RPC before it waits to publish terminal state under mu.
	cancelMu sync.Mutex
	cancel   context.CancelFunc

	// Tests can lower these limits to exercise the blocking boundary without
	// constructing a production-sized window.
	windowBytes  uint64
	windowFrames int
}

type nativeInputPendingFrame struct {
	rows  uint64
	bytes uint64
	frame []byte
	mp    *mpool.MPool
}

func (e *Execution) NewNativeInput(streamRef []byte) (*NativeInput, error) {
	if e == nil || e.runtime == nil || len(e.ticket) != ticketBytes || len(streamRef) != 32 {
		return nil, internalErrorf("sidecar flight: invalid native input identity")
	}
	input := &NativeInput{
		execution:    e,
		streamRef:    append([]byte(nil), streamRef...),
		windowBytes:  maxNativeInputWindowBytes,
		windowFrames: maxNativeInputWindowFrames,
	}
	e.mu.Lock()
	if e.started || e.cleanupRunning || e.terminal || e.quiesced {
		e.mu.Unlock()
		return nil, internalErrorf("sidecar flight: execution no longer accepts native inputs")
	}
	if len(e.inputs) >= maxNativeInputs {
		e.mu.Unlock()
		return nil, internalErrorf("sidecar flight: native input count exceeds the protocol limit")
	}
	for _, existing := range e.inputs {
		if bytes.Equal(existing.streamRef, streamRef) {
			e.mu.Unlock()
			return nil, internalErrorf("sidecar flight: duplicate native input identity")
		}
	}
	e.inputs = append(e.inputs, input)
	e.mu.Unlock()
	return input, nil
}

func (n *NativeInput) open(ctx context.Context) error {
	if n.stream != nil {
		return nil
	}
	streamCtx := ctx
	if streamCtx == nil {
		streamCtx = context.Background()
	}
	streamCtx, cancel := context.WithCancel(streamCtx)
	n.setCancel(cancel)
	stream, err := n.execution.runtime.conn.NewStream(streamCtx, putStream, doPutMethod)
	if err != nil {
		n.cancelStream()
		return internalErrorf("sidecar flight: open native input stream: %w", err)
	}
	request, err := proto.Marshal(&uploadInputRequest{Ticket: n.execution.ticket, StreamRef: n.streamRef})
	if err != nil {
		n.cancelStream()
		return internalErrorf("sidecar flight: encode native input request: %w", err)
	}
	if err = stream.SendMsg(&flightData{Descriptor: &flightDescriptor{Type: commandDescriptor, Cmd: request}}); err != nil {
		n.cancelStream()
		return internalErrorf("sidecar flight: send native input descriptor: %w", err)
	}
	n.stream = stream
	ack, err := n.recvAck()
	if err != nil {
		n.cancelStream()
		return err
	}
	if ack.AcknowledgedBatches != 0 || ack.Rows != 0 || ack.Bytes != 0 || ack.Complete || ack.NotNeeded || !ack.Ready {
		n.cancelStream()
		return internalErrorf("sidecar flight: invalid native input attachment acknowledgement")
	}
	return nil
}

// Start attaches the input to its prepared execution. All inputs are attached
// before DoGet starts so a sidecar plan that prunes a read cannot retire its
// ticket before the matching DoPut handler exists.
func (n *NativeInput) Start(ctx context.Context) error {
	if n == nil {
		return internalErrorf("sidecar flight: nil native input")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.retired.Load() || n.notNeeded {
		return nil
	}
	if n.finished || n.terminalErr != nil {
		return errors.Join(internalErrorf("sidecar flight: native input is terminal"), n.terminalErr)
	}
	if err := n.open(ctx); err != nil {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = err
		return err
	}
	return nil
}

func (n *NativeInput) Send(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) error {
	if n == nil || bat == nil || bat.IsEmpty() {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.retired.Load() || n.notNeeded {
		return nil
	}
	if n.finished || n.terminalErr != nil {
		return errors.Join(internalErrorf("sidecar flight: native input is terminal"), n.terminalErr)
	}
	if mp == nil {
		err := internalErrorf("sidecar flight: native input has no query memory pool")
		n.failPendingLocked(err)
		return err
	}
	if err := n.open(ctx); err != nil {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = err
		return err
	}
	if err := bat.CheckLength(); err != nil {
		n.failPendingLocked(err)
		return err
	}
	if (len(bat.Attrs) != 0 && len(bat.Attrs) != len(bat.Vecs)) || len(bat.ExtraBuf) != 0 ||
		bat.Recursive != 0 || bat.ShuffleIDX != 0 {
		err := internalErrorf("sidecar flight: native input contains unsupported batch metadata")
		n.failPendingLocked(err)
		return err
	}
	size, err := bat.MarshalBinarySize()
	if err != nil {
		n.failPendingLocked(err)
		return err
	}
	limit := min(maxNativeInputBatchBytes, n.execution.runtime.config.MaxBatchBytes)
	if uint64(size) > limit {
		err = n.sendSplitLocked(bat, limit, mp)
		if err != nil {
			n.failPendingLocked(err)
		}
		return err
	}
	frame, err := marshalNativeInputFrame(n.sequence+1, bat, size, mp)
	if err != nil {
		n.failPendingLocked(err)
		return err
	}
	return n.sendFrameLocked(frame, uint64(bat.RowCount()), uint64(size), mp)
}

// sendFrameLocked takes ownership of frame and keeps it query-accounted until
// its cumulative consumed acknowledgement or a terminal send/receive outcome.
func (n *NativeInput) sendFrameLocked(
	frame []byte,
	frameRows uint64,
	payloadBytes uint64,
	mp *mpool.MPool,
) error {
	windowBytes := n.windowBytes
	if windowBytes == 0 {
		windowBytes = maxNativeInputWindowBytes
	}
	if payloadBytes > windowBytes {
		mp.Free(frame)
		err := internalErrorf("sidecar flight: native input frame exceeds the send window")
		n.failPendingLocked(err)
		return err
	}
	windowFrames := n.windowFrames
	if windowFrames <= 0 || windowFrames > len(n.pending) {
		windowFrames = maxNativeInputWindowFrames
	}
	for n.pendingCount != 0 &&
		(n.pendingCount >= windowFrames || payloadBytes > windowBytes-n.pendingBytes) {
		terminal, err := n.receiveConsumedLocked()
		if err != nil {
			mp.Free(frame)
			n.failPendingLocked(err)
			return err
		}
		if terminal {
			mp.Free(frame)
			return nil
		}
	}
	n.sequence++
	if err := n.stream.SendMsg(&flightData{AppMetadata: frame}); err != nil {
		mp.Free(frame)
		if n.retired.Load() {
			n.releasePendingLocked()
			return nil
		}
		err = internalErrorf("sidecar flight: send native input batch: %w", err)
		n.failPendingLocked(err)
		return err
	}
	index := (n.pendingHead + n.pendingCount) % len(n.pending)
	n.pending[index] = nativeInputPendingFrame{
		rows: frameRows, bytes: payloadBytes, frame: frame, mp: mp,
	}
	n.pendingCount++
	n.pendingBytes += payloadBytes
	return nil
}

// receiveConsumedLocked consumes exactly one ordered server response. Normal
// responses release the oldest query-accounted frame. A terminal not-needed
// response releases any frames already accepted by gRPC but never consumed.
func (n *NativeInput) receiveConsumedLocked() (bool, error) {
	ack, err := n.recvAck()
	if err != nil {
		if n.retired.Load() {
			n.releasePendingLocked()
			return true, nil
		}
		return false, err
	}
	if ack.NotNeeded {
		acknowledged := n.sequence - uint64(n.pendingCount)
		if !ack.Complete || ack.Ready || ack.AcknowledgedBatches != acknowledged ||
			ack.Rows != n.rows || ack.Bytes != n.bytes {
			return false, internalErrorf("sidecar flight: invalid native input not-needed acknowledgement")
		}
		var trailing flightPutResult
		if err = n.stream.RecvMsg(&trailing); err != io.EOF {
			if n.retired.Load() {
				n.releasePendingLocked()
				return true, nil
			}
			return false, internalErrorf(
				"sidecar flight: native input not-needed stream has trailing results: %w", err)
		}
		n.releasePendingLocked()
		n.sequence = ack.AcknowledgedBatches
		n.rows, n.bytes = ack.Rows, ack.Bytes
		n.notNeeded = true
		n.finished = true
		n.cancelStream()
		return true, nil
	}
	if n.pendingCount == 0 {
		return false, internalErrorf("sidecar flight: unexpected native input acknowledgement")
	}
	frame := n.pending[n.pendingHead]
	expectedBatches := n.sequence - uint64(n.pendingCount) + 1
	expectedRows := n.rows + frame.rows
	expectedBytes := n.bytes + frame.bytes
	if ack.AcknowledgedBatches != expectedBatches || ack.Rows != expectedRows || ack.Bytes != expectedBytes ||
		ack.Complete || ack.NotNeeded || ack.Ready {
		return false, internalErrorf("sidecar flight: invalid native input acknowledgement")
	}
	if frame.frame != nil {
		frame.mp.Free(frame.frame)
	}
	n.pending[n.pendingHead] = nativeInputPendingFrame{}
	n.pendingHead = (n.pendingHead + 1) % len(n.pending)
	n.pendingCount--
	if n.pendingCount == 0 {
		n.pendingHead = 0
	}
	n.pendingBytes -= frame.bytes
	n.rows = ack.Rows
	n.bytes = ack.Bytes
	return false, nil
}

func (n *NativeInput) releasePendingLocked() {
	for offset := 0; offset < n.pendingCount; offset++ {
		index := (n.pendingHead + offset) % len(n.pending)
		if n.pending[index].frame != nil {
			n.pending[index].mp.Free(n.pending[index].frame)
		}
		n.pending[index] = nativeInputPendingFrame{}
	}
	n.pendingHead = 0
	n.pendingCount = 0
	n.pendingBytes = 0
}

func (n *NativeInput) failPendingLocked(err error) {
	if n.terminalErr == nil {
		n.terminalErr = err
	}
	n.releasePendingLocked()
	n.cancelStream()
}

func (n *NativeInput) sendSplitLocked(source *batch.Batch, limit uint64, mp *mpool.MPool) error {
	for start := 0; start < source.RowCount(); {
		plan, err := planNativeWindow(source, start, limit)
		if err != nil {
			return err
		}
		frame, err := func() ([]byte, error) {
			window, cloneErr := cloneNativeWindow(source, start, plan.end, mp)
			if cloneErr != nil {
				return nil, cloneErr
			}
			defer window.Clean(mp)
			actualBytes, sizeErr := window.MarshalBinarySize()
			if sizeErr != nil {
				return nil, sizeErr
			}
			if actualBytes != plan.payloadBytes {
				return nil, internalErrorf(
					"sidecar flight: native input split plan mismatch: planned=%d actual=%d",
					plan.payloadBytes, actualBytes,
				)
			}
			return marshalNativeInputFrame(n.sequence+1, window, actualBytes, mp)
		}()
		if err != nil {
			return err
		}
		if err = n.sendFrameLocked(
			frame,
			uint64(plan.end-start),
			uint64(plan.payloadBytes),
			mp,
		); err != nil {
			return err
		}
		if n.retired.Load() || n.notNeeded {
			return nil
		}
		start = plan.end
	}
	return nil
}

func cloneNativeWindow(source *batch.Batch, start, end int, mp *mpool.MPool) (*batch.Batch, error) {
	result := batch.NewWithSize(len(source.Vecs))
	result.Attrs = append([]string(nil), source.Attrs...)
	for i, sourceVec := range source.Vecs {
		if sourceVec == nil {
			result.Clean(mp)
			return nil, internalErrorf("sidecar flight: native input contains a nil vector")
		}
		cloneStart, cloneEnd := start, end
		if sourceVec.IsConst() {
			cloneStart, cloneEnd = 0, min(1, sourceVec.Length())
		}
		cloned, err := sourceVec.CloneWindow(cloneStart, cloneEnd, mp)
		if err != nil {
			result.Clean(mp)
			return nil, err
		}
		if sourceVec.IsConst() {
			cloned.SetLength(end - start)
		}
		result.Vecs[i] = cloned
	}
	result.SetRowCount(end - start)
	return result, nil
}

func (n *NativeInput) Finish(ctx context.Context) error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.retired.Load() {
		return nil
	}
	if n.finished {
		return n.terminalErr
	}
	if n.terminalErr != nil {
		n.finished = true
		return n.terminalErr
	}
	if err := n.open(ctx); err != nil {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = err
		n.finished = true
		return err
	}
	for n.pendingCount != 0 {
		terminal, err := n.receiveConsumedLocked()
		if err != nil {
			n.failPendingLocked(err)
			n.finished = true
			return err
		}
		if terminal {
			return nil
		}
	}
	if err := n.stream.CloseSend(); err != nil {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = internalErrorf("sidecar flight: close native input: %w", err)
		n.finished = true
		return n.terminalErr
	}
	ack, err := n.recvAck()
	if err != nil {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = err
		n.finished = true
		return err
	}
	if !ack.Complete || ack.Ready || ack.AcknowledgedBatches != n.sequence || ack.Rows != n.rows ||
		ack.Bytes != n.bytes {
		n.terminalErr = internalErrorf("sidecar flight: missing final native input acknowledgement")
		n.finished = true
		return n.terminalErr
	}
	var trailing flightPutResult
	if err = n.stream.RecvMsg(&trailing); err != io.EOF {
		if n.retired.Load() {
			return nil
		}
		n.terminalErr = internalErrorf("sidecar flight: native input stream has trailing results: %w", err)
		n.finished = true
		return n.terminalErr
	}
	n.rows, n.bytes = ack.Rows, ack.Bytes
	n.finished = true
	n.cancelStream()
	return nil
}

// Retire publishes successful result-side EOF to the producer and interrupts
// any blocked DoPut operation. Unlike Abort it does not manufacture a query
// error: the sidecar has already produced the complete result and no longer
// consumes this input.
func (n *NativeInput) Retire() {
	if n == nil {
		return
	}
	n.retired.Store(true)
	n.cancelStream()
	n.mu.Lock()
	n.releasePendingLocked()
	n.mu.Unlock()
}

func (n *NativeInput) recvAck() (*uploadInputAck, error) {
	result := new(flightPutResult)
	if err := n.stream.RecvMsg(result); err != nil {
		return nil, internalErrorf("sidecar flight: receive native input acknowledgement: %w", err)
	}
	ack := new(uploadInputAck)
	if len(result.AppMetadata) == 0 || proto.Unmarshal(result.AppMetadata, ack) != nil {
		return nil, internalErrorf("sidecar flight: malformed native input acknowledgement")
	}
	return ack, nil
}

func (n *NativeInput) Abort(cause error) {
	if n == nil {
		return
	}
	n.cancelStream()
	if n.retired.Load() {
		return
	}
	if cause == nil {
		cause = context.Canceled
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.releasePendingLocked()
	if n.retired.Load() {
		return
	}
	if n.terminalErr == nil {
		n.terminalErr = cause
	}
	n.finished = true
}

func (n *NativeInput) setCancel(cancel context.CancelFunc) {
	n.cancelMu.Lock()
	n.cancel = cancel
	n.cancelMu.Unlock()
}

func (n *NativeInput) cancelStream() {
	n.cancelMu.Lock()
	cancel := n.cancel
	n.cancel = nil
	n.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (n *NativeInput) Err() error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.terminalErr
}

func (n *NativeInput) NotNeeded() bool {
	if n == nil {
		return false
	}
	if n.retired.Load() {
		return true
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.notNeeded
}
