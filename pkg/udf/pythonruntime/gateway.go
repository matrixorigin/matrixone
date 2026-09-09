// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package pythonruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ModeScalar      = "SCALAR"
	ModeVector      = "VECTOR"
	NullCallHandler = "CALLED_ON_NULL_INPUT"
	NullReturnNull  = "RETURNS_NULL_ON_NULL_INPUT"
	statusOK        = "OK"
)

type Gateway struct {
	cfg       ClientConfig
	conn      *grpc.ClientConn
	flight    flight.FlightServiceClient
	mu        sync.Mutex
	lifecycle sync.RWMutex
}

func NewGateway(cfg ClientConfig) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if cfg.MaxBatchRows == 0 {
		cfg.MaxBatchRows = DefaultMaxBatchRows
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	return &Gateway{cfg: cfg}, nil
}

func (g *Gateway) Language() string { return udf.LanguagePython }

func (g *Gateway) connect() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.flight != nil {
		return nil
	}
	conn, err := grpc.NewClient(g.cfg.ServerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(int(g.cfg.MaxBatchBytes)+1<<20),
			grpc.MaxCallRecvMsgSize(int(g.cfg.MaxBatchBytes)+1<<20),
		),
	)
	if err != nil {
		return err
	}
	g.conn = conn
	g.flight = flight.NewFlightServiceClient(conn)
	return nil
}

func (g *Gateway) Close() error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn == nil {
		return nil
	}
	err := g.conn.Close()
	g.conn = nil
	g.flight = nil
	return err
}

type openPayload struct {
	Handler        string            `json:"handler"`
	Source         string            `json:"source"`
	Mode           string            `json:"mode"`
	NullPolicy     string            `json:"null_policy"`
	ABIContract    string            `json:"abi_contract"`
	AdapterVersion string            `json:"adapter_version"`
	SDKVersion     string            `json:"sdk_version"`
	Context        map[string]string `json:"context,omitempty"`
	Args           []TypeDescriptor  `json:"args"`
	Return         TypeDescriptor    `json:"return"`
	MaxBatchBytes  int64             `json:"max_batch_bytes"`
	MaxBatchRows   int64             `json:"max_batch_rows"`
}

func (g *Gateway) Execute(ctx context.Context, invocation *udf.Invocation, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	if invocation == nil || result == nil || mp == nil {
		return fmt.Errorf("python runtime: nil invocation, result, or memory pool")
	}
	if err := validateInvocation(invocation); err != nil {
		return err
	}
	if invocation.Length == 0 {
		return result.PreExtendAndReset(0)
	}
	if err := g.connect(); err != nil {
		return err
	}

	args := make([]TypeDescriptor, len(invocation.Args))
	for i, typ := range invocation.Args {
		var err error
		args[i], err = NewTypeDescriptor(typ)
		if err != nil {
			return err
		}
	}
	returnDescriptor, err := NewTypeDescriptor(invocation.ReturnType)
	if err != nil {
		return err
	}
	openBody, err := json.Marshal(openPayload{Handler: invocation.Handler, Source: invocation.Source, Mode: invocation.Mode, NullPolicy: invocation.NullPolicy, ABIContract: invocation.ABIContract, AdapterVersion: invocation.AdapterVersion, SDKVersion: invocation.SDKVersion, Context: cloneMap(invocation.Context), Args: args, Return: returnDescriptor, MaxBatchBytes: g.cfg.MaxBatchBytes, MaxBatchRows: g.cfg.MaxBatchRows})
	if err != nil {
		return fmt.Errorf("python runtime: encode open payload: %w", err)
	}
	open, err := protocol.MarshalControl(protocol.Control{Kind: "OpenInvocation", Tuple: invocation.Tuple, Payload: openBody})
	if err != nil {
		return err
	}

	stream, err := g.flight.DoExchange(streamCtx)
	if err != nil {
		return fmt.Errorf("python runtime: open Arrow Flight exchange: %w", err)
	}
	// Flight carries the invocation command in the descriptor.  The descriptor
	// is delivered to the server separately from the Arrow IPC reader; putting
	// the JSON command in AppMetadata on this message makes PyArrow try to
	// decode it as an IPC message.
	if err := stream.Send(&flight.FlightData{FlightDescriptor: &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: open}}); err != nil {
		return err
	}
	if err := result.PreExtendAndReset(invocation.Length); err != nil {
		return err
	}

	var sequence protocol.Sequence
	var schemaFrame *ArrowFrame
	var inputSchema []byte
	rows := 0
	var start int64
	var batchIndex uint64
	var lastSequence uint64
	var inputEnded bool
	for start < int64(invocation.Length) {
		batch, err := encodeInputBatch(invocation.Inputs, invocation.Args, start, int64(invocation.Length)-start, g.cfg.MaxBatchBytes, g.cfg.MaxBatchRows)
		if err != nil {
			return err
		}
		if len(batch.Frames) != 2 {
			return fmt.Errorf("python runtime: input batch contains %d Arrow frames", len(batch.Frames))
		}
		if batchIndex == 0 {
			inputSchema = append([]byte(nil), batch.Frames[0].Header...)
			if err := stream.Send(&flight.FlightData{DataHeader: batch.Frames[0].Header}); err != nil {
				return err
			}
		} else if !bytes.Equal(inputSchema, batch.Frames[0].Header) {
			return fmt.Errorf("python runtime: input batches do not share one Arrow schema")
		}
		sequenceNumber := batchIndex + 1
		if err := sequence.AcceptInput(sequenceNumber); err != nil {
			return err
		}
		inputControl, err := protocol.MarshalControl(protocol.Control{Kind: "InputBatch", Tuple: invocation.Tuple, Sequence: sequenceNumber})
		if err != nil {
			return err
		}
		if err := stream.Send(&flight.FlightData{DataHeader: batch.Frames[1].Header, DataBody: batch.Frames[1].Body, AppMetadata: inputControl}); err != nil {
			return err
		}
		lastSequence = sequenceNumber
		isLastBatch := start+batch.Rows == int64(invocation.Length)
		if isLastBatch {
			// EndInput is an application-level half-close.  Send it before
			// waiting for the final result so the runtime must keep its result
			// direction alive when that result is delayed on the wire.
			endInput, err := protocol.MarshalControl(protocol.Control{Kind: "EndInput", Tuple: invocation.Tuple, LastSequence: lastSequence})
			if err != nil {
				return err
			}
			if err := stream.Send(&flight.FlightData{AppMetadata: endInput}); err != nil {
				return err
			}
			if err := sequence.EndInput(lastSequence); err != nil {
				return err
			}
			if err := stream.CloseSend(); err != nil {
				return err
			}
			inputEnded = true
		}
		batchRows, err := g.receiveResultBatch(streamCtx, stream, invocation.Tuple, sequenceNumber, batch.Rows, &schemaFrame, returnDescriptor, result, mp, &sequence)
		if err != nil {
			return err
		}
		rows += batchRows
		start += batch.Rows
		batchIndex++
		if isLastBatch {
			break
		}
	}
	if !inputEnded || lastSequence != batchIndex {
		return fmt.Errorf("python runtime: input stream did not reach EndInput")
	}
	return g.receiveFinish(streamCtx, stream, invocation.Tuple, invocation.Length, rows, &sequence)
}

func encodeInputBatch(inputs []*vector.Vector, args []types.Type, start, remaining, maxBytes, maxRows int64) (encodedRecordBatch, error) {
	if start < 0 || remaining <= 0 {
		return encodedRecordBatch{}, fmt.Errorf("invalid Python UDF input batch range")
	}
	if maxRows <= 0 {
		return encodedRecordBatch{}, fmt.Errorf("invalid Python UDF max batch rows")
	}
	if remaining > maxRows {
		remaining = maxRows
	}
	tryEncode := func(rows int64) ([]ArrowFrame, error) {
		record, _, err := BuildInputRecordRange(inputs, args, int(start), int(rows))
		if err != nil {
			return nil, err
		}
		frames, encodeErr := EncodeRecordBatch(record, maxBytes)
		record.Release()
		return frames, encodeErr
	}
	best, probe := int64(0), int64(1)
	var bestFrames []ArrowFrame
	failedAt := int64(0)
	for {
		frames, err := tryEncode(probe)
		if err == nil {
			best, bestFrames = probe, frames
			if probe == remaining {
				return encodedRecordBatch{Frames: bestFrames, Rows: best}, nil
			}
			if probe > remaining/2 {
				probe = remaining
			} else {
				probe *= 2
			}
			continue
		}
		if !errors.Is(err, errArrowBatchTooLarge) {
			return encodedRecordBatch{}, err
		}
		failedAt = probe
		break
	}
	low, high := best+1, failedAt-1
	for low <= high {
		middle := low + (high-low)/2
		frames, err := tryEncode(middle)
		if err == nil {
			best, bestFrames = middle, frames
			low = middle + 1
			continue
		}
		if !errors.Is(err, errArrowBatchTooLarge) {
			return encodedRecordBatch{}, err
		}
		high = middle - 1
	}
	if best == 0 {
		return encodedRecordBatch{}, fmt.Errorf("%w: one Arrow row exceeds %d bytes", errArrowBatchTooLarge, maxBytes)
	}
	return encodedRecordBatch{Frames: bestFrames, Rows: best}, nil
}

func (g *Gateway) receiveResultBatch(
	ctx context.Context,
	stream flight.FlightService_DoExchangeClient,
	tuple protocol.FencingTuple,
	sequenceNumber uint64,
	expectedRows int64,
	schemaFrame **ArrowFrame,
	returnDescriptor TypeDescriptor,
	result vector.FunctionResultWrapper,
	mp *mpool.MPool,
	sequence *protocol.Sequence,
) (int, error) {
	for {
		data, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return 0, fmt.Errorf("python runtime: exchange ended before result sequence %d", sequenceNumber)
			}
			return 0, fmt.Errorf("python runtime: receive result: %w", err)
		}
		if data == nil || (len(data.AppMetadata) == 0 && len(data.DataHeader) == 0) {
			return 0, fmt.Errorf("python runtime: empty Flight result")
		}
		if len(data.DataHeader) > 0 {
			if *schemaFrame == nil {
				if len(data.DataBody) != 0 {
					return 0, fmt.Errorf("python runtime: result schema contains a body")
				}
				*schemaFrame = &ArrowFrame{Header: append([]byte(nil), data.DataHeader...)}
				if len(data.AppMetadata) > 0 {
					control, err := protocol.UnmarshalControl(data.AppMetadata)
					if err != nil || control.Kind != "ResultSchema" {
						return 0, fmt.Errorf("python runtime: invalid result schema metadata")
					}
				}
				continue
			}
			control, err := protocol.UnmarshalControl(data.AppMetadata)
			if err != nil {
				return 0, err
			}
			if control.Tuple != tuple || control.Kind != "ResultBatch" || control.Sequence != sequenceNumber {
				return 0, fmt.Errorf("python runtime: unexpected result sequence %d", control.Sequence)
			}
			if err := sequence.AcceptResult(control.Sequence); err != nil {
				return 0, err
			}
			snapshot, err := protocol.FreezeOutput(data.DataBody, g.cfg.MaxBatchBytes)
			if err != nil {
				return 0, err
			}
			if err := snapshot.Validate(len(data.DataBody), snapshot.Digest()); err != nil {
				return 0, err
			}
			decoded, err := DecodeRecordBatch(**schemaFrame, ArrowFrame{Header: append([]byte(nil), data.DataHeader...), Body: snapshot.TrustedBytes()}, g.cfg.MaxBatchBytes)
			if err != nil {
				return 0, fmt.Errorf("python runtime: decode result: %w", err)
			}
			if decoded.NumCols() != 1 || decoded.NumRows() <= 0 || decoded.NumRows() != expectedRows {
				decoded.Release()
				return 0, fmt.Errorf("python runtime: result batch row count %d, expected %d", decoded.NumRows(), expectedRows)
			}
			if decoded.Schema().Field(0).Name != "result" {
				decoded.Release()
				return 0, fmt.Errorf("python runtime: result field has unexpected name %q", decoded.Schema().Field(0).Name)
			}
			if err := returnDescriptor.ValidateField(decoded.Schema().Field(0)); err != nil {
				decoded.Release()
				return 0, err
			}
			if err := AppendArrowResult(returnDescriptor, decoded.Column(0), result, mp); err != nil {
				decoded.Release()
				return 0, err
			}
			decoded.Release()
			if err := g.action(ctx, "AcknowledgeResults", protocol.Control{Kind: "AcknowledgeResults", Tuple: tuple, AckSequence: control.Sequence}); err != nil {
				return 0, err
			}
			if err := sequence.AcknowledgeResults(control.Sequence); err != nil {
				return 0, err
			}
			return int(expectedRows), nil
		}
		control, err := protocol.UnmarshalControl(data.AppMetadata)
		if err != nil {
			return 0, err
		}
		if control.Tuple != tuple {
			return 0, fmt.Errorf("python runtime: result tuple changed")
		}
		switch control.Kind {
		case "ResultSchema":
			continue
		case "InputConsumed":
			if control.Sequence != sequenceNumber {
				return 0, fmt.Errorf("python runtime: input sequence %d was consumed while waiting for result %d", control.Sequence, sequenceNumber)
			}
			continue
		case "Error":
			return 0, fmt.Errorf("python runtime: worker error: %s", control.Reason)
		case "Finish":
			return 0, fmt.Errorf("python runtime: Finish arrived before result sequence %d", sequenceNumber)
		default:
			return 0, fmt.Errorf("python runtime: unexpected control %q", control.Kind)
		}
	}
}

func (g *Gateway) receiveFinish(
	ctx context.Context,
	stream flight.FlightService_DoExchangeClient,
	tuple protocol.FencingTuple,
	expectedRows, rows int,
	sequence *protocol.Sequence,
) error {
	for {
		data, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("python runtime: exchange ended before Finish")
			}
			return fmt.Errorf("python runtime: receive Finish: %w", err)
		}
		if data == nil || len(data.DataHeader) > 0 {
			return fmt.Errorf("python runtime: unexpected Arrow data while finishing")
		}
		control, err := protocol.UnmarshalControl(data.AppMetadata)
		if err != nil {
			return err
		}
		if control.Tuple != tuple {
			return fmt.Errorf("python runtime: Finish tuple changed")
		}
		switch control.Kind {
		case "InputConsumed", "ResultSchema":
			continue
		case "Error":
			return fmt.Errorf("python runtime: worker error: %s", control.Reason)
		case "Finish":
			lastInput, lastResult, acked, inputEnded := sequence.State()
			if control.Status != statusOK || control.FinishID == "" || rows != expectedRows ||
				!inputEnded || control.LastSequence != lastInput || lastResult != lastInput || acked != lastInput || !sequence.ReadyToFinish() {
				return fmt.Errorf("python runtime: invalid Finish status=%q rows=%d", control.Status, rows)
			}
			return g.action(ctx, "AcknowledgeFinish", protocol.Control{Kind: "AcknowledgeFinish", Tuple: tuple, FinishID: control.FinishID})
		default:
			return fmt.Errorf("python runtime: unexpected control %q", control.Kind)
		}
	}
}

func validateInvocation(invocation *udf.Invocation) error {
	if invocation.Language != "" && invocation.Language != udf.LanguagePython {
		return fmt.Errorf("python runtime: unsupported language %q", invocation.Language)
	}
	if invocation.Handler == "" || invocation.Source == "" {
		return fmt.Errorf("python runtime: handler and source are required")
	}
	if invocation.Length < 0 || len(invocation.Args) != len(invocation.Inputs) {
		return fmt.Errorf("python runtime: invalid input shape")
	}
	if invocation.Mode == "" {
		invocation.Mode = ModeScalar
	}
	if invocation.Mode != ModeScalar && invocation.Mode != ModeVector {
		return fmt.Errorf("python runtime: unsupported mode %q", invocation.Mode)
	}
	if invocation.NullPolicy == "" {
		invocation.NullPolicy = NullCallHandler
	}
	if invocation.NullPolicy != NullCallHandler && invocation.NullPolicy != NullReturnNull {
		return fmt.Errorf("python runtime: unsupported NULL policy %q", invocation.NullPolicy)
	}
	if invocation.ABIContract == "" {
		invocation.ABIContract = udf.PythonABIContract
	}
	if invocation.AdapterVersion == "" {
		invocation.AdapterVersion = udf.PythonAdapterVersion
	}
	if invocation.SDKVersion == "" {
		invocation.SDKVersion = udf.PythonSDKVersion
	}
	if invocation.ABIContract != udf.PythonABIContract || invocation.AdapterVersion != udf.PythonAdapterVersion {
		return fmt.Errorf("python runtime: unsupported Python ABI contract %q/%q", invocation.ABIContract, invocation.AdapterVersion)
	}
	return invocation.Tuple.Validate()
}

func (g *Gateway) action(ctx context.Context, kind string, control protocol.Control) error {
	body, err := protocol.MarshalControl(control)
	if err != nil {
		return err
	}
	actionCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	stream, err := g.flight.DoAction(actionCtx, &flight.Action{Type: kind, Body: body})
	if err != nil {
		return fmt.Errorf("python runtime: %s: %w", kind, err)
	}
	result, err := stream.Recv()
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("python runtime: empty %s response", kind)
	}
	ack, err := protocol.UnmarshalControl(result.Body)
	if err != nil {
		return err
	}
	if ack.Status != statusOK {
		return fmt.Errorf("python runtime: %s rejected: %s", kind, ack.Reason)
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("python runtime: %s returned multiple results", kind)
		}
		return err
	}
	return nil
}

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}
