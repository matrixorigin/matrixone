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
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"google.golang.org/grpc"
)

const nativeBatchFrameHeaderBytes = 24

var putStream = &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}

// NativeInput is one single-use, acknowledged MO-batch stream for a StreamRead.
// Send returns only after the sidecar scan has fully consumed that batch.
type NativeInput struct {
	execution *Execution
	streamRef []byte

	mu          sync.Mutex
	stream      grpc.ClientStream
	sequence    uint64
	rows        uint64
	bytes       uint64
	finished    bool
	notNeeded   bool
	terminalErr error

	// cancelMu is intentionally independent from mu. Send and Finish hold mu
	// while waiting for a sidecar acknowledgement; Abort must be able to cancel
	// that RPC before it waits to publish terminal state under mu.
	cancelMu sync.Mutex
	cancel   context.CancelFunc
}

func (e *Execution) NewNativeInput(streamRef []byte) (*NativeInput, error) {
	if e == nil || e.runtime == nil || len(e.ticket) != ticketBytes || len(streamRef) != 32 {
		return nil, internalErrorf("sidecar flight: invalid native input identity")
	}
	input := &NativeInput{execution: e, streamRef: append([]byte(nil), streamRef...)}
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
	if n.notNeeded {
		return nil
	}
	if n.finished || n.terminalErr != nil {
		return errors.Join(internalErrorf("sidecar flight: native input is terminal"), n.terminalErr)
	}
	if err := n.open(ctx); err != nil {
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
	if n.notNeeded {
		return nil
	}
	if n.finished || n.terminalErr != nil {
		return errors.Join(internalErrorf("sidecar flight: native input is terminal"), n.terminalErr)
	}
	if err := n.open(ctx); err != nil {
		n.terminalErr = err
		return err
	}
	if err := bat.CheckLength(); err != nil {
		n.terminalErr = err
		return err
	}
	if (len(bat.Attrs) != 0 && len(bat.Attrs) != len(bat.Vecs)) || len(bat.ExtraBuf) != 0 ||
		bat.Recursive != 0 || bat.ShuffleIDX != 0 {
		n.terminalErr = internalErrorf("sidecar flight: native input contains unsupported batch metadata")
		return n.terminalErr
	}
	size, err := bat.MarshalBinarySize()
	if err != nil {
		n.terminalErr = err
		return err
	}
	limit := min(maxNativeInputBatchBytes, n.execution.runtime.config.MaxBatchBytes)
	if uint64(size) > limit {
		err = n.sendSplitLocked(bat, limit, mp)
		if err != nil {
			n.terminalErr = err
		}
		return err
	}
	payload, err := bat.MarshalBinary()
	if err != nil {
		n.terminalErr = err
		return err
	}
	return n.sendPayloadLocked(payload)
}

func (n *NativeInput) sendPayloadLocked(payload []byte) error {
	n.sequence++
	frame := make([]byte, nativeBatchFrameHeaderBytes+len(payload))
	copy(frame[:4], "MOB1")
	binary.LittleEndian.PutUint16(frame[4:6], 1)
	binary.LittleEndian.PutUint64(frame[8:16], n.sequence)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(len(payload)))
	copy(frame[nativeBatchFrameHeaderBytes:], payload)
	if err := n.stream.SendMsg(&flightData{AppMetadata: frame}); err != nil {
		n.terminalErr = internalErrorf("sidecar flight: send native input batch: %w", err)
		return n.terminalErr
	}
	ack, err := n.recvAck()
	if err != nil {
		n.terminalErr = err
		return err
	}
	if ack.NotNeeded {
		acknowledgedCurrent := ack.AcknowledgedBatches == n.sequence
		acknowledgedPrevious := ack.AcknowledgedBatches == n.sequence-1
		expectedRows, expectedBytes := n.rows, n.bytes
		if acknowledgedCurrent {
			expectedRows += binary.LittleEndian.Uint64(payload[:8])
			expectedBytes += uint64(len(payload))
		}
		if !ack.Complete || ack.Ready || (!acknowledgedCurrent && !acknowledgedPrevious) ||
			ack.Rows != expectedRows || ack.Bytes != expectedBytes {
			n.terminalErr = internalErrorf("sidecar flight: invalid native input not-needed acknowledgement")
			return n.terminalErr
		}
		var trailing flightPutResult
		if err = n.stream.RecvMsg(&trailing); err != io.EOF {
			n.terminalErr = internalErrorf("sidecar flight: native input not-needed stream has trailing results: %w", err)
			return n.terminalErr
		}
		n.sequence = ack.AcknowledgedBatches
		n.rows, n.bytes = ack.Rows, ack.Bytes
		n.notNeeded = true
		n.finished = true
		n.cancelStream()
		return nil
	}
	expectedRows := n.rows + binary.LittleEndian.Uint64(payload[:8])
	expectedBytes := n.bytes + uint64(len(payload))
	if ack.AcknowledgedBatches != n.sequence || ack.Rows != expectedRows || ack.Bytes != expectedBytes ||
		ack.Complete || ack.NotNeeded || ack.Ready {
		n.terminalErr = internalErrorf("sidecar flight: invalid native input acknowledgement")
		return n.terminalErr
	}
	n.rows = ack.Rows
	n.bytes = ack.Bytes
	return nil
}

func (n *NativeInput) sendSplitLocked(source *batch.Batch, limit uint64, mp *mpool.MPool) error {
	for start := 0; start < source.RowCount(); {
		low, high, best := start+1, source.RowCount(), -1
		for low <= high {
			middle := low + (high-low)/2
			window, err := cloneNativeWindow(source, start, middle, mp)
			if err != nil {
				return err
			}
			size, sizeErr := window.MarshalBinarySize()
			window.Clean(mp)
			if sizeErr != nil {
				return sizeErr
			}
			if uint64(size) <= limit {
				best = middle
				low = middle + 1
			} else {
				high = middle - 1
			}
		}
		if best < 0 {
			return internalErrorf("sidecar flight: one native input row exceeds the negotiated limit")
		}
		window, err := cloneNativeWindow(source, start, best, mp)
		if err != nil {
			return err
		}
		payload, marshalErr := window.MarshalBinary()
		window.Clean(mp)
		if marshalErr != nil {
			return marshalErr
		}
		if err = n.sendPayloadLocked(payload); err != nil {
			return err
		}
		if n.notNeeded {
			return nil
		}
		start = best
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
	if n.finished {
		return n.terminalErr
	}
	if n.terminalErr != nil {
		n.finished = true
		return n.terminalErr
	}
	if err := n.open(ctx); err != nil {
		n.terminalErr = err
		n.finished = true
		return err
	}
	if err := n.stream.CloseSend(); err != nil {
		n.terminalErr = internalErrorf("sidecar flight: close native input: %w", err)
		n.finished = true
		return n.terminalErr
	}
	ack, err := n.recvAck()
	if err != nil {
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
		n.terminalErr = internalErrorf("sidecar flight: native input stream has trailing results: %w", err)
		n.finished = true
		return n.terminalErr
	}
	n.rows, n.bytes = ack.Rows, ack.Bytes
	n.finished = true
	n.cancelStream()
	return nil
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
	if cause == nil {
		cause = context.Canceled
	}
	n.mu.Lock()
	defer n.mu.Unlock()
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
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.notNeeded
}
