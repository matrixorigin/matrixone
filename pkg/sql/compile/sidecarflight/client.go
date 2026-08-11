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
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	flightService       = "/arrow.flight.protocol.FlightService/"
	getFlightInfoMethod = flightService + "GetFlightInfo"
	doGetMethod         = flightService + "DoGet"
	doActionMethod      = flightService + "DoAction"
	commandDescriptor   = int32(2)
	ticketBytes         = 32
	protocolVersion     = uint32(2)
	substraitVersion    = "0.78.0"
)

var serverStream = &grpc.StreamDesc{ServerStreams: true}

// Config bounds both the wire payload and the lifetime of a sidecar request.
// TLSConfig must identify the sidecar and include the CN client certificate.
type Config struct {
	Address        string
	TLSConfig      *tls.Config
	MaxBatchBytes  uint64
	RequestTimeout time.Duration
	CleanupTimeout time.Duration
}

// Runtime owns one negotiated Flight connection and every execution prepared
// through it. Close first stops admission, then quiesces all tickets, and only
// then closes the transport.
type Runtime struct {
	config         Config
	capabilityHash [sha256.Size]byte
	conn           *grpc.ClientConn

	mu         sync.Mutex
	stopped    bool
	executions map[*Execution]struct{}
	preparing  map[*preparation]struct{}
	closeDone  chan struct{}
	closeErr   error
}

type preparation struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Execution owns a single-use Flight ticket. The caller must invoke either
// FinishSuccess after EOF or CancelAndJoin before releasing read leases.
type Execution struct {
	runtime *Runtime
	ticket  []byte
	schema  *Schema

	mu             sync.Mutex
	streamCancel   context.CancelFunc
	started        bool
	terminal       bool
	cleanupRunning bool
	cleanupDone    chan struct{}
	cleanupErr     error
}

// quiescenceUnknownError means the client could not prove that a possibly
// prepared sidecar execution has stopped. Its read leases must be retained.
type quiescenceUnknownError struct {
	err error
}

func (e *quiescenceUnknownError) Error() string { return e.err.Error() }
func (e *quiescenceUnknownError) Unwrap() error { return e.err }

// NewRuntime establishes the mTLS channel and performs exact capability
// negotiation before returning it to the CN lifecycle owner.
func NewRuntime(ctx context.Context, config Config, capabilityDocument string) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.Address == "" || config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify ||
		len(config.TLSConfig.Certificates) == 0 || config.TLSConfig.RootCAs == nil {
		return nil, fmt.Errorf("sidecar flight: address, client certificate, and server CA are required")
	}
	if config.MaxBatchBytes == 0 || config.RequestTimeout <= 0 || config.CleanupTimeout <= 0 || capabilityDocument == "" {
		return nil, fmt.Errorf("sidecar flight: invalid transport limits")
	}
	tlsConfig := config.TLSConfig.Clone()
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	if config.MaxBatchBytes > uint64(maxInt())-(1<<20) {
		return nil, fmt.Errorf("sidecar flight: max batch bytes overflows platform int")
	}
	maximumMessage := config.MaxBatchBytes + 1<<20
	conn, err := grpc.NewClient(
		config.Address,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(maximumMessage))),
	)
	if err != nil {
		return nil, fmt.Errorf("sidecar flight: create client: %w", err)
	}
	runtime := &Runtime{
		config: config, conn: conn, executions: make(map[*Execution]struct{}), preparing: make(map[*preparation]struct{}),
		capabilityHash: sha256.Sum256([]byte(capabilityDocument)),
	}
	negotiationCtx, cancel := context.WithTimeout(ctx, config.RequestTimeout)
	defer cancel()
	capabilities, err := runtime.doAction(negotiationCtx, "GetCapabilities", nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("sidecar flight: negotiate capabilities: %w", err)
	}
	if !bytes.Equal(capabilities, []byte(capabilityDocument)) {
		_ = conn.Close()
		return nil, fmt.Errorf("sidecar flight: capability document mismatch")
	}
	return runtime, nil
}

// Prepare exchanges an already-admitted Substrait plan for a single-use
// ticket and validates the result schema before the caller exposes metadata.
func (r *Runtime) Prepare(ctx context.Context, accountID uint64, queryID, plan []byte, outputTypes []planpb.Type, headings []string) (*Execution, error) {
	if r == nil {
		return nil, fmt.Errorf("sidecar flight: nil runtime")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if accountID == 0 || len(queryID) != 16 || len(plan) == 0 || len(plan) > 16<<20 {
		return nil, fmt.Errorf("sidecar flight: query identity and Substrait plan are required")
	}
	deadline := time.Now().Add(r.config.RequestTimeout)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	prepareCtx, cancelPrepare := context.WithDeadline(ctx, deadline)
	preparing := &preparation{cancel: cancelPrepare, done: make(chan struct{})}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		cancelPrepare()
		return nil, fmt.Errorf("sidecar flight: runtime is stopping")
	}
	if r.preparing == nil {
		r.preparing = make(map[*preparation]struct{})
	}
	r.preparing[preparing] = struct{}{}
	r.mu.Unlock()
	defer func() {
		cancelPrepare()
		r.mu.Lock()
		delete(r.preparing, preparing)
		close(preparing.done)
		r.mu.Unlock()
	}()
	idempotencyKey := executionIdempotencyKey(accountID, queryID, plan)
	request := &executeSubstraitRequest{
		ProtocolVersion: protocolVersion, SubstraitVersion: substraitVersion,
		CapabilityHash: r.capabilityHash[:], MaxBatchBytes: r.config.MaxBatchBytes,
		DeadlineUnixMS: uint64(deadline.UnixMilli()), Plan: plan,
		QueryID: append([]byte(nil), queryID...), IdempotencyKey: idempotencyKey[:],
		AccountID: accountID,
	}
	command, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("sidecar flight: encode request: %w", err)
	}
	descriptor := &flightDescriptor{Type: commandDescriptor, Cmd: command}
	info := new(flightInfo)
	for attempt := 0; attempt < 3; attempt++ {
		info.Reset()
		err = r.conn.Invoke(prepareCtx, getFlightInfoMethod, descriptor, info)
		if err == nil || status.Code(err) != codes.Unavailable || prepareCtx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, r.failAmbiguousPrepare(fmt.Errorf("sidecar flight: prepare: %w", err), idempotencyKey[:])
	}
	if len(info.Endpoint) != 1 || info.Endpoint[0] == nil || info.Endpoint[0].Ticket == nil || len(info.Endpoint[0].Ticket.Ticket) != ticketBytes {
		return nil, r.failAmbiguousPrepare(fmt.Errorf("sidecar flight: malformed endpoint ticket"), idempotencyKey[:])
	}
	ticket := append([]byte(nil), info.Endpoint[0].Ticket.Ticket...)
	if len(info.Endpoint[0].Locations) != 0 {
		return nil, r.failPrepared(fmt.Errorf("sidecar flight: endpoint redirection is not allowed"), ticket)
	}
	if !bytes.Equal(info.AppMetadata, r.capabilityHash[:]) {
		return nil, r.failPrepared(fmt.Errorf("sidecar flight: response capability hash mismatch"), ticket)
	}
	schema, err := ParseSchema(info.Schema, outputTypes, headings)
	if err != nil {
		return nil, r.failPrepared(fmt.Errorf("sidecar flight: validate schema: %w", err), ticket)
	}
	execution := &Execution{
		runtime:     r,
		ticket:      ticket,
		schema:      schema,
		cleanupDone: make(chan struct{}),
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil, r.failPrepared(fmt.Errorf("sidecar flight: runtime is stopping"), ticket)
	}
	r.executions[execution] = struct{}{}
	r.mu.Unlock()
	return execution, nil
}

func executionIdempotencyKey(accountID uint64, queryID, plan []byte) [sha256.Size]byte {
	planHash := sha256.Sum256(plan)
	input := make([]byte, 8, 8+len(queryID)+len(planHash))
	binary.LittleEndian.PutUint64(input, accountID)
	input = append(input, queryID...)
	input = append(input, planHash[:]...)
	return sha256.Sum256(input)
}

func (r *Runtime) failPrepared(primary error, ticket []byte) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), r.config.CleanupTimeout)
	defer cancel()
	if cleanupErr := r.cancel(cleanupCtx, ticket, nil); cleanupErr != nil {
		return &quiescenceUnknownError{err: errors.Join(primary, cleanupErr)}
	}
	return primary
}

func (r *Runtime) failAmbiguousPrepare(primary error, idempotencyKey []byte) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), r.config.CleanupTimeout)
	defer cancel()
	if cleanupErr := r.cancel(cleanupCtx, nil, idempotencyKey); cleanupErr != nil {
		return &quiescenceUnknownError{err: errors.Join(primary, cleanupErr)}
	}
	return primary
}

func (r *Runtime) cancel(ctx context.Context, ticket, idempotencyKey []byte) error {
	request := &cancelExecutionRequest{Ticket: ticket, IdempotencyKey: idempotencyKey}
	body, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("sidecar flight: encode cancellation: %w", err)
	}
	response, err := r.doAction(ctx, "CancelExecution", body)
	if err != nil {
		return fmt.Errorf("sidecar flight: cancel execution: %w", err)
	}
	if string(response) != "quiesced" && string(response) != "not-found" {
		return fmt.Errorf("sidecar flight: unexpected cancellation acknowledgement %q", response)
	}
	return nil
}

func (r *Runtime) remove(execution *Execution) {
	r.mu.Lock()
	delete(r.executions, execution)
	r.mu.Unlock()
}

// Close stops new preparations and quiesces every prepared or streaming
// execution before closing the shared gRPC connection.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.stopped {
		done := r.closeDone
		r.mu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
			r.mu.Lock()
			err := r.closeErr
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	r.stopped = true
	r.closeDone = make(chan struct{})
	executions := make([]*Execution, 0, len(r.executions))
	for execution := range r.executions {
		executions = append(executions, execution)
	}
	preparing := make([]*preparation, 0, len(r.preparing))
	for request := range r.preparing {
		preparing = append(preparing, request)
	}
	r.mu.Unlock()
	var result error
	for _, request := range preparing {
		request.cancel()
	}
	for _, request := range preparing {
		select {
		case <-request.done:
		case <-ctx.Done():
			result = errors.Join(result, context.Cause(ctx))
		}
	}
	for _, execution := range executions {
		cleanupCtx, cancel := boundedContext(ctx, r.config.CleanupTimeout)
		if err := execution.CancelAndJoin(cleanupCtx); err != nil {
			result = errors.Join(result, err)
		}
		cancel()
	}
	if err := r.conn.Close(); err != nil {
		result = errors.Join(result, err)
	}
	r.mu.Lock()
	r.closeErr = result
	close(r.closeDone)
	r.mu.Unlock()
	return result
}

func (r *Runtime) doAction(ctx context.Context, actionType string, body []byte) ([]byte, error) {
	stream, err := r.conn.NewStream(ctx, serverStream, doActionMethod)
	if err != nil {
		return nil, err
	}
	if err = stream.SendMsg(&flightAction{Type: actionType, Body: body}); err != nil {
		return nil, err
	}
	if err = stream.CloseSend(); err != nil {
		return nil, err
	}
	result := new(flightResult)
	if err = stream.RecvMsg(result); err != nil {
		return nil, err
	}
	extra := new(flightResult)
	if err = stream.RecvMsg(extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("sidecar flight: action returned multiple results")
		}
		return nil, err
	}
	return result.Body, nil
}

// IsPreVisibilityFallback reports failures for which no schema or row was
// exposed and native execution is safe. Invalid plans, authentication errors,
// and malformed responses remain terminal because retrying them would hide a
// contract or security defect.
func IsPreVisibilityFallback(err error) bool {
	if err == nil {
		return false
	}
	if IsQuiescenceUnknown(err) {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	}
	message := err.Error()
	return strings.Contains(message, "UNSUPPORTED_PLAN") ||
		strings.Contains(message, "UNSUPPORTED_VERSION") ||
		strings.Contains(message, "CAPABILITY_MISMATCH")
}

// IsQuiescenceUnknown reports that a sidecar execution may still be using its
// admitted snapshot. Callers must retain its GC leases.
func IsQuiescenceUnknown(err error) bool {
	var unknown *quiescenceUnknownError
	return errors.As(err, &unknown)
}

func maxInt() int { return int(^uint(0) >> 1) }

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	deadline := time.Now().Add(timeout)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(context.WithoutCancel(parent), deadline)
}
