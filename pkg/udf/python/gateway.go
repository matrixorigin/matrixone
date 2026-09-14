// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package python

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
	ModeScalar                      = "SCALAR"
	ModeVector                      = "VECTOR"
	NullCallHandler                 = "CALLED_ON_NULL_INPUT"
	NullReturnNull                  = "RETURNS_NULL_ON_NULL_INPUT"
	DefaultMaxActiveInvocations     = 8
	DefaultMaxInvocationRows        = 1 << 20
	DefaultMaxInvocationResultBytes = 256 << 20
	DefaultMaxTerminalEntries       = 8192
	DefaultMaxTerminalBytes         = 8 << 20
	DefaultTerminalRecordTTL        = 10 * time.Minute
	// These limits are part of the current worker capability contract. H is
	// the worker-wide child budget; account and owner limits are narrower
	// fairness fences enforced before a handler child is started.
	DefaultWorkerMaxHandlerProcesses = 8
	DefaultWorkerMaxAccountHandlers  = 8
	DefaultWorkerMaxOwnerHandlers    = 4
	statusOK                         = "OK"
)

var errGatewayClosed = errors.New("python udf gateway is closed")

var _ udf.RuntimeDefinitionValidator = (*Gateway)(nil)

type Gateway struct {
	cfg               ClientConfig
	artifactResolver  ArtifactResolver
	allowInlineSource bool
	conn              *grpc.ClientConn
	flight            flight.FlightServiceClient
	mu                sync.Mutex
	capabilityMu      sync.Mutex
	admissionMu       sync.Mutex
	capabilityReady   bool
	workerLeaseEpoch  uint64
	closed            bool
	active            chan struct{}
	ledger            *protocol.TerminalLedger
	ledgerTTL         time.Duration
	// admittedGroups and closedGroups are the Gateway's ownership fence for
	// the current one-member adapter. A group epoch may be admitted only once;
	// a terminal tombstone is reclaimable only after its exact group epoch has
	// been closed by this owner. closedGroups stores a monotonic high-water
	// epoch per group so a late lower-epoch retry cannot become executable
	// after its ledger tombstone is collected.
	admittedGroups       map[string]uint64
	closedGroups         map[string]uint64
	closedGroupBytes     int64
	reservedGroupBytes   int64
	closedGroupLimit     int
	closedGroupByteLimit int64
}

func NewGateway(cfg ClientConfig) (*Gateway, error) {
	// NewGateway is kept for direct adapter tests and local development. CN
	// production construction uses NewGatewayWithArtifactStore so an
	// executable plan can never supply source bytes itself.
	return newGateway(cfg, nil, true)
}

// NewGatewayWithArtifactStore constructs the CN data-plane client for the
// current typed contract. Source is resolved from the exact account/digest
// reference before Open is sent to the worker; a plan carrying source is
// rejected at this boundary.
func NewGatewayWithArtifactStore(cfg ClientConfig, resolver ArtifactResolver) (*Gateway, error) {
	if resolver == nil {
		return nil, fmt.Errorf("RESOURCE_UNAVAILABLE: Python artifact resolver is not configured")
	}
	return newGateway(cfg, resolver, false)
}

func newGateway(cfg ClientConfig, resolver ArtifactResolver, allowInlineSource bool) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if cfg.MaxBatchRows == 0 {
		cfg.MaxBatchRows = DefaultMaxBatchRows
	}
	if cfg.MaxInvocationRows == 0 {
		cfg.MaxInvocationRows = DefaultMaxInvocationRows
	}
	if cfg.MaxInvocationResultBytes == 0 {
		cfg.MaxInvocationResultBytes = DefaultMaxInvocationResultBytes
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.MaxActiveInvocations == 0 {
		cfg.MaxActiveInvocations = DefaultMaxActiveInvocations
	}
	if cfg.MaxTerminalEntries == 0 {
		cfg.MaxTerminalEntries = DefaultMaxTerminalEntries
	}
	if cfg.MaxTerminalBytes == 0 {
		cfg.MaxTerminalBytes = DefaultMaxTerminalBytes
	}
	if cfg.TerminalRecordTTL == 0 {
		cfg.TerminalRecordTTL = DefaultTerminalRecordTTL
	}
	ledger, err := protocol.NewTerminalLedger(cfg.MaxTerminalEntries, cfg.MaxTerminalBytes)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		cfg:                  cfg,
		artifactResolver:     resolver,
		allowInlineSource:    allowInlineSource,
		active:               make(chan struct{}, cfg.MaxActiveInvocations),
		ledger:               ledger,
		ledgerTTL:            cfg.TerminalRecordTTL,
		admittedGroups:       make(map[string]uint64),
		closedGroups:         make(map[string]uint64),
		closedGroupLimit:     cfg.MaxTerminalEntries,
		closedGroupByteLimit: cfg.MaxTerminalBytes,
	}, nil
}

func (g *Gateway) Language() string { return udf.LanguagePython }

func (g *Gateway) CheckLanguageReady(ctx context.Context, language string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if language != udf.LanguagePython {
		return fmt.Errorf("python udf: unsupported readiness language %q", language)
	}
	if !g.cfg.Enabled {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime is disabled")
	}
	if !g.cfg.AllowUnisolated {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime requires explicit unisolated opt-in")
	}
	client, err := g.flightClient()
	if err != nil {
		return err
	}
	return g.ensureCapabilities(ctx, client)
}

// ValidateDefinition asks the current worker to compile the exact artifact
// that a CREATE or REPLACE is about to publish. This action has no
// FunctionRef, invocation lease, handler slot, or ledger entry: a definition
// has not acquired a Catalog identity and must not consume execution state.
// The worker performs compile(source, ..., "exec") only; it does not execute
// top-level code, import dependencies, or call the handler.
func (g *Gateway) ValidateDefinition(ctx context.Context, definition *udf.RoutineDefinition) error {
	g.mu.Lock()
	closed := g.closed
	g.mu.Unlock()
	if closed {
		return errGatewayClosed
	}
	if !g.cfg.Enabled {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime is disabled")
	}
	if !g.cfg.AllowUnisolated {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime requires explicit unisolated opt-in")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if definition == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: nil Python routine definition")
	}
	if definition.Language != "" && definition.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported UDF language %q", definition.Language)
	}
	if strings.TrimSpace(definition.Handler) == "" {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python handler is required")
	}
	if strings.Contains(definition.Handler, ":") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python external handler import requires the immutable artifact catalog")
	}
	if definition.DefinitionSchemaVersion != udf.PythonDefinitionSchemaVersion ||
		definition.ABIContract != udf.PythonABIContract ||
		definition.AdapterVersion != udf.PythonAdapterVersion ||
		definition.SDKVersion != udf.PythonSDKVersion {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition contract is not supported")
	}
	if definition.Mode != ModeScalar && definition.Mode != ModeVector {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported Python mode %q", definition.Mode)
	}
	if definition.NullPolicy != NullCallHandler && definition.NullPolicy != NullReturnNull {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported Python NULL policy %q", definition.NullPolicy)
	}
	if !udf.IsSHA256Digest(definition.ArtifactDigest) || !udf.IsSHA256Digest(definition.EnvironmentDigest) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition digest is invalid")
	}
	currentEnvironment, err := udf.PythonEnvironmentDigest()
	if err != nil || currentEnvironment != definition.EnvironmentDigest {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python environment digest does not match the current contract")
	}
	if !udf.IsSHA256Digest(definition.DefinitionFingerprint) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint is invalid")
	}
	args := make([]TypeDescriptor, len(definition.Args))
	for index, typ := range definition.Args {
		args[index], err = NewTypeDescriptor(typ)
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python argument descriptor %d: %w", index, err)
		}
	}
	returnDescriptor, err := NewTypeDescriptor(definition.ReturnType)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python return descriptor: %w", err)
	}
	expectedFingerprint, err := DefinitionFingerprint(
		definition.DefinitionSchemaVersion,
		definition.Handler, definition.Mode, definition.NullPolicy,
		definition.ABIContract, definition.AdapterVersion,
		definition.ArtifactDigest, definition.EnvironmentDigest, definition.SDKVersion,
		args, returnDescriptor,
	)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint cannot be computed: %w", err)
	}
	if definition.DefinitionFingerprint != expectedFingerprint {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint does not match the typed definition")
	}

	source := definition.Source
	if g.artifactResolver != nil {
		resolved, resolveErr := g.artifactResolver.Resolve(
			ctx, definition.AccountID, definition.Handler, definition.ArtifactDigest,
		)
		if resolveErr != nil {
			return resolveErr
		}
		if source != "" && source != resolved {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python artifact source does not match the immutable artifact")
		}
		source = resolved
	} else if !g.allowInlineSource {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python artifact resolver is not configured")
	}
	if source == "" || !utf8.ValidString(source) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python artifact source is empty or not valid UTF-8")
	}
	if int64(len([]byte(source))) > DefaultMaxArtifactBytes {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python artifact exceeds %d bytes", DefaultMaxArtifactBytes)
	}
	if udf.PythonInlineArtifactDigest(definition.Handler, source) != definition.ArtifactDigest {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python artifact digest does not match the immutable source")
	}

	client, err := g.flightClient()
	if err != nil {
		return err
	}
	if err := g.ensureCapabilities(ctx, client); err != nil {
		return err
	}
	payload, err := json.Marshal(definitionValidationPayload{
		AccountID:               definition.AccountID,
		Handler:                 definition.Handler,
		Source:                  source,
		Mode:                    definition.Mode,
		NullPolicy:              definition.NullPolicy,
		ABIContract:             definition.ABIContract,
		AdapterVersion:          definition.AdapterVersion,
		SDKVersion:              definition.SDKVersion,
		DefinitionSchemaVersion: definition.DefinitionSchemaVersion,
		ArtifactDigest:          definition.ArtifactDigest,
		EnvironmentDigest:       definition.EnvironmentDigest,
		DefinitionFingerprint:   definition.DefinitionFingerprint,
		Args:                    args,
		Return:                  returnDescriptor,
	})
	if err != nil {
		return fmt.Errorf("python udf: encode definition validation: %w", err)
	}
	actionCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	stream, err := client.DoAction(actionCtx, &flight.Action{Type: "ValidatePythonDefinition", Body: payload})
	if err != nil {
		g.invalidateCapabilities()
		return fmt.Errorf("python udf: definition validation action: %w", err)
	}
	result, err := stream.Recv()
	if err != nil {
		g.invalidateCapabilities()
		return fmt.Errorf("python udf: definition validation response: %w", err)
	}
	if result == nil {
		g.invalidateCapabilities()
		return fmt.Errorf("python udf: empty definition validation response")
	}
	var response definitionValidationResponse
	decoder := json.NewDecoder(bytes.NewReader(result.Body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		g.invalidateCapabilities()
		return fmt.Errorf("python udf: decode definition validation response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		g.invalidateCapabilities()
		return fmt.Errorf("python udf: definition validation response has trailing JSON")
	}
	if response.Status == "ERROR" {
		if _, err := stream.Recv(); err != io.EOF {
			g.invalidateCapabilities()
			if err == nil {
				return fmt.Errorf("python udf: definition validation returned multiple results")
			}
			return fmt.Errorf("python udf: definition validation stream: %w", err)
		}
		if response.Reason == "" {
			return fmt.Errorf("USER_CODE: Python definition validation failed")
		}
		return errors.New(response.Reason)
	}
	if response.Status != statusOK || response.ArtifactDigest != definition.ArtifactDigest ||
		response.DefinitionFingerprint != definition.DefinitionFingerprint {
		g.invalidateCapabilities()
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition validation returned an invalid result")
	}
	if _, err := stream.Recv(); err != io.EOF {
		g.invalidateCapabilities()
		if err == nil {
			return fmt.Errorf("python udf: definition validation returned multiple results")
		}
		return fmt.Errorf("python udf: definition validation stream: %w", err)
	}
	return nil
}

func (g *Gateway) connect() error {
	_, err := g.flightClient()
	return err
}

func (g *Gateway) flightClient() (flight.FlightServiceClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errGatewayClosed
	}
	if g.flight != nil {
		return g.flight, nil
	}
	conn, err := grpc.NewClient(g.cfg.ServerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(int(g.cfg.MaxBatchBytes)+1<<20),
			grpc.MaxCallRecvMsgSize(int(g.cfg.MaxBatchBytes)+1<<20),
		),
	)
	if err != nil {
		return nil, err
	}
	g.conn = conn
	g.flight = flight.NewFlightServiceClient(conn)
	return g.flight, nil
}

func (g *Gateway) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	conn := g.conn
	g.conn = nil
	g.mu.Unlock()
	if conn == nil {
		return nil
	}
	// Closing the connection is deliberately outside the mutex. gRPC uses
	// this operation to interrupt in-flight exchanges; holding a lifecycle
	// read lock until Execute returns would make shutdown wait for the very RPC
	// it needs to cancel.
	return conn.Close()
}

type openPayload struct {
	FunctionRef              udf.FunctionRef       `json:"function_ref"`
	Handler                  string                `json:"handler"`
	Source                   string                `json:"source"`
	Mode                     string                `json:"mode"`
	NullPolicy               string                `json:"null_policy"`
	ABIContract              string                `json:"abi_contract"`
	AdapterVersion           string                `json:"adapter_version"`
	SDKVersion               string                `json:"sdk_version"`
	DefinitionSchemaVersion  int                   `json:"definition_schema_version"`
	ArtifactDigest           string                `json:"artifact_digest"`
	EnvironmentDigest        string                `json:"environment_digest"`
	DefinitionFingerprint    string                `json:"definition_fingerprint"`
	Context                  map[string]string     `json:"context,omitempty"`
	CallsiteID               string                `json:"callsite_id"`
	MayError                 bool                  `json:"may_error"`
	SecurityMode             string                `json:"security_mode"`
	Leakproof                bool                  `json:"leakproof"`
	StatementContext         *udf.StatementContext `json:"statement_context,omitempty"`
	SecurityFrame            *udf.SecurityFrame    `json:"security_frame,omitempty"`
	Args                     []TypeDescriptor      `json:"args"`
	Return                   TypeDescriptor        `json:"return"`
	MaxBatchBytes            int64                 `json:"max_batch_bytes"`
	MaxBatchRows             int64                 `json:"max_batch_rows"`
	MaxInvocationRows        int64                 `json:"max_invocation_rows"`
	MaxInvocationResultBytes int64                 `json:"max_invocation_result_bytes"`
	HandlerTimeoutSeconds    float64               `json:"handler_timeout_seconds"`
}

func (g *Gateway) Execute(ctx context.Context, invocation *udf.Invocation, result vector.FunctionResultWrapper, mp *mpool.MPool) (err error) {
	g.mu.Lock()
	closed := g.closed
	g.mu.Unlock()
	if closed {
		return errGatewayClosed
	}
	if !g.cfg.Enabled {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime is disabled")
	}
	if !g.cfg.AllowUnisolated {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime requires explicit unisolated opt-in")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	if invocation == nil || result == nil || mp == nil {
		return fmt.Errorf("python udf: nil invocation, result, or memory pool")
	}
	execution := *invocation
	if g.artifactResolver != nil {
		if execution.Source != "" {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python invocation contains source; use its immutable artifact digest")
		}
		// Validate the identity, semantic contract, descriptors, and resource
		// shape before resolving an account-scoped artifact.  Resolution is a
		// read with tenant-sensitive error/latency behavior; it must not become
		// the first operation performed on an untrusted or stale plan.  The
		// source-dependent digest check is repeated after the resolver returns.
		if err := validateInvocationHeader(&execution); err != nil {
			return err
		}
		execution.Source, err = g.artifactResolver.Resolve(
			streamCtx,
			execution.FunctionRef.AccountID,
			execution.Handler,
			execution.ArtifactDigest,
		)
		if err != nil {
			return err
		}
	} else if !g.allowInlineSource && execution.Source == "" {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python artifact resolver is not configured")
	}
	if err := validateInvocation(&execution); err != nil {
		return err
	}
	args := make([]TypeDescriptor, len(execution.Args))
	for i, typ := range execution.Args {
		args[i], err = NewTypeDescriptor(typ)
		if err != nil {
			return err
		}
	}
	returnDescriptor, err := NewTypeDescriptor(invocation.ReturnType)
	if err != nil {
		return err
	}
	if err := validateInvocationBudget(executionBudget{
		rows:           int64(execution.Length),
		maxRows:        g.cfg.MaxInvocationRows,
		returnType:     execution.ReturnType,
		maxResultBytes: g.cfg.MaxInvocationResultBytes,
	}); err != nil {
		return err
	}
	if execution.Length == 0 {
		return result.PreExtendAndReset(0)
	}
	inputEncoder, err := newInputBatchEncoder(execution.Inputs, execution.Args)
	if err != nil {
		return err
	}
	inputEncoderClosed := false
	closeInputEncoder := func() error {
		if inputEncoderClosed {
			return nil
		}
		if err := inputEncoder.close(); err != nil {
			return err
		}
		inputEncoderClosed = true
		return nil
	}
	defer func() {
		if closeErr := closeInputEncoder(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	client, err := g.flightClient()
	if err != nil {
		return err
	}
	if err := g.ensureCapabilities(streamCtx, client); err != nil {
		return err
	}
	// The worker instance owns the lease epoch. Copy the invocation before
	// attaching it so a caller cannot observe or reuse an identity issued for
	// another worker process. If the worker was restarted after the cached
	// handshake, the exchange fails with STALE_LEASE_EPOCH and the deferred
	// invalidation below forces a fresh handshake on the next call.
	g.mu.Lock()
	workerLeaseEpoch := g.workerLeaseEpoch
	g.mu.Unlock()
	if workerLeaseEpoch == 0 {
		return fmt.Errorf("python udf: capability handshake did not return a worker lease epoch")
	}
	execution.Tuple.LeaseEpoch = workerLeaseEpoch
	if err := validateInvocation(&execution); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			g.invalidateCapabilities()
		}
	}()
	openBody, err := json.Marshal(openPayload{
		FunctionRef: execution.FunctionRef, Handler: execution.Handler, Source: execution.Source,
		Mode: execution.Mode, NullPolicy: execution.NullPolicy, ABIContract: execution.ABIContract,
		AdapterVersion: execution.AdapterVersion, SDKVersion: execution.SDKVersion,
		DefinitionSchemaVersion: execution.DefinitionSchemaVersion,
		ArtifactDigest:          execution.ArtifactDigest, EnvironmentDigest: execution.EnvironmentDigest,
		DefinitionFingerprint: execution.DefinitionFingerprint,
		Context:               cloneMap(execution.Context), CallsiteID: execution.CallsiteID,
		MayError: execution.MayError, SecurityMode: execution.SecurityMode,
		Leakproof: execution.Leakproof, StatementContext: execution.StatementContext,
		SecurityFrame: execution.SecurityFrame, Args: args, Return: returnDescriptor,
		MaxBatchBytes: g.cfg.MaxBatchBytes, MaxBatchRows: g.cfg.MaxBatchRows,
		MaxInvocationRows: g.cfg.MaxInvocationRows, MaxInvocationResultBytes: g.cfg.MaxInvocationResultBytes,
		HandlerTimeoutSeconds: g.cfg.RequestTimeout.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("python udf: encode open payload: %w", err)
	}
	open, err := protocol.MarshalControl(protocol.Control{Kind: "OpenInvocation", Tuple: execution.Tuple, Payload: openBody})
	if err != nil {
		return err
	}
	admission, err := g.admitInvocation(&execution)
	if err != nil {
		return err
	}
	openAttempted := false
	defer func() {
		reason := admissionReason(streamCtx, openAttempted, err)
		if cleanupErr := admission.finish(openAttempted, reason); err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()

	stream, err := client.DoExchange(streamCtx)
	if err != nil {
		return fmt.Errorf("python udf: open Arrow Flight exchange: %w", err)
	}
	// Flight carries the invocation command in the descriptor.  The descriptor
	// is delivered to the server separately from the Arrow IPC reader; putting
	// the JSON command in AppMetadata on this message makes PyArrow try to
	// decode it as an IPC message.
	// Once Send is attempted the remote side may have accepted the command. The
	// admission is therefore terminalized conservatively even when Send returns
	// a transport error; transparent replay would violate VOLATILE semantics.
	openAttempted = true
	if err := stream.Send(&flight.FlightData{FlightDescriptor: &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: open}}); err != nil {
		return err
	}
	if err := result.PreExtendAndReset(execution.Length); err != nil {
		return err
	}

	var sequence protocol.Sequence
	var schemaFrame *ArrowFrame
	schemaReady := false
	var inputSchema []byte
	rows := 0
	var start int64
	var batchIndex uint64
	var lastSequence uint64
	var inputEnded bool
	for start < int64(execution.Length) {
		batch, err := encodeInputBatchWithEncoder(inputEncoder, start, int64(execution.Length)-start, g.cfg.MaxBatchBytes, g.cfg.MaxBatchRows)
		if err != nil {
			return err
		}
		if len(batch.Frames) != 2 {
			return fmt.Errorf("python udf: input batch contains %d Arrow frames", len(batch.Frames))
		}
		if batchIndex == 0 {
			inputSchema = append([]byte(nil), batch.Frames[0].Header...)
			if err := stream.Send(&flight.FlightData{DataHeader: batch.Frames[0].Header}); err != nil {
				return err
			}
		} else if !bytes.Equal(inputSchema, batch.Frames[0].Header) {
			return fmt.Errorf("python udf: input batches do not share one Arrow schema")
		}
		sequenceNumber := batchIndex + 1
		if err := sequence.AcceptInput(sequenceNumber); err != nil {
			return err
		}
		inputControl, err := protocol.MarshalControl(protocol.Control{Kind: "InputBatch", Tuple: execution.Tuple, Sequence: sequenceNumber})
		if err != nil {
			return err
		}
		if err := stream.Send(&flight.FlightData{DataHeader: batch.Frames[1].Header, DataBody: batch.Frames[1].Body, AppMetadata: inputControl}); err != nil {
			return err
		}
		lastSequence = sequenceNumber
		isLastBatch := start+batch.Rows == int64(execution.Length)
		if isLastBatch {
			// EndInput is an application-level half-close.  Send it before
			// waiting for the final result so the runtime must keep its result
			// direction alive when that result is delayed on the wire.
			endInput, err := protocol.MarshalControl(protocol.Control{Kind: "EndInput", Tuple: execution.Tuple, LastSequence: lastSequence})
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
		batchRows, err := g.receiveResultBatch(streamCtx, client, stream, execution.Tuple, sequenceNumber, batch.Rows, &schemaFrame, &schemaReady, returnDescriptor, result, mp, &sequence)
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
		return fmt.Errorf("python udf: input stream did not reach EndInput")
	}
	// Close the local Arrow writer before accepting the worker's terminal
	// Finish.  If finalizing the local input owner fails, the admission cleanup
	// still has a chance to cancel/close the remote invocation; acknowledging
	// Finish first would publish remote success while returning a local error.
	if err := closeInputEncoder(); err != nil {
		return err
	}
	if err := g.receiveFinish(streamCtx, client, stream, execution.Tuple, execution.Length, rows, &sequence); err != nil {
		return err
	}
	return nil
}

// invocationAdmission is the real Gateway owner for the CN-side execution
// resources. K is the active slot, the group owns its release, and the ledger
// retains the fencing key after completion so a late retry cannot create a new
// execution. The current W=1 exchange has one member per physical invocation;
// the group API remains able to grow to a multi-member scheduler later.
type invocationAdmission struct {
	gateway      *Gateway
	group        *protocol.ExecutionGroup
	credit       *protocol.LedgerCredit
	key          string
	invocationID string

	mu         sync.Mutex
	finished   bool
	memberOpen bool
}

func (g *Gateway) admitInvocation(invocation *udf.Invocation) (*invocationAdmission, error) {
	if invocation == nil {
		return nil, fmt.Errorf("python udf: cannot admit a nil invocation")
	}
	if err := invocation.Tuple.Validate(); err != nil {
		return nil, err
	}
	if err := g.ensureAdmissionState(); err != nil {
		return nil, err
	}

	key, err := invocationLedgerKey(invocation.Tuple)
	if err != nil {
		return nil, err
	}
	g.admissionMu.Lock()
	ledger := g.ledger
	ttl := g.ledgerTTL
	active := g.active
	if err := g.claimGroupLocked(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch); err != nil {
		g.admissionMu.Unlock()
		return nil, err
	}
	g.admissionMu.Unlock()
	if ledger == nil || active == nil {
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, fmt.Errorf("python udf: admission state is not initialized")
	}
	// Expire is intentionally limited to tombstones by TerminalLedger. An
	// active invocation can leave this path only through finish/abandon.
	// TTL alone cannot prove that a stale tuple can no longer receive a grant.
	// The group owner supplies that proof; a closed epoch is the only fence
	// that permits an expired tombstone to release its reserved capacity.
	ledger.Expire(time.Now(), g.epochClosed)
	entryBytes := int64(len(key))
	credit, err := ledger.Reserve(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch, 1, entryBytes)
	if err != nil {
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF terminal ledger: %w", err)
	}
	if err := credit.Add(key, entryBytes, time.Now().Add(ttl)); err != nil {
		credit.ReleaseUnused()
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, err
	}
	select {
	case active <- struct{}{}:
		// The group release callback returns this slot after the member is
		// terminal. No queue is created, so caller vectors are never held while
		// waiting for K.
	default:
		_ = ledger.Abandon(key)
		credit.ReleaseUnused()
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF active invocation slots are full")
	}

	var releaseOnce sync.Once
	release := func() error {
		releaseOnce.Do(func() {
			<-active
		})
		credit.ReleaseUnused()
		return nil
	}
	group, err := protocol.NewExecutionGroup(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch, 1, release)
	if err != nil {
		<-active
		_ = ledger.Abandon(key)
		credit.ReleaseUnused()
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, err
	}
	token, err := group.BeginOpen(invocation.Tuple.InvocationID)
	if err != nil {
		_ = group.Close(protocol.ReasonPartialOpenError)
		_ = ledger.Abandon(key)
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, err
	}
	if err := token.Commit(); err != nil {
		_ = group.Close(protocol.ReasonPartialOpenError)
		_ = ledger.Abandon(key)
		g.releaseGroupClaim(invocation.Tuple.GroupID, invocation.Tuple.GroupEpoch)
		return nil, err
	}
	return &invocationAdmission{
		gateway:      g,
		group:        group,
		credit:       credit,
		key:          key,
		invocationID: invocation.Tuple.InvocationID,
		memberOpen:   true,
	}, nil
}

func (g *Gateway) ensureAdmissionState() error {
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	if g.active == nil {
		maxActive := g.cfg.MaxActiveInvocations
		if maxActive <= 0 {
			maxActive = DefaultMaxActiveInvocations
		}
		g.active = make(chan struct{}, maxActive)
	}
	if g.ledger == nil {
		maxEntries := g.cfg.MaxTerminalEntries
		if maxEntries <= 0 {
			maxEntries = DefaultMaxTerminalEntries
		}
		maxBytes := g.cfg.MaxTerminalBytes
		if maxBytes <= 0 {
			maxBytes = DefaultMaxTerminalBytes
		}
		ledger, err := protocol.NewTerminalLedger(maxEntries, maxBytes)
		if err != nil {
			return err
		}
		g.ledger = ledger
	}
	if g.ledgerTTL <= 0 {
		g.ledgerTTL = g.cfg.TerminalRecordTTL
		if g.ledgerTTL <= 0 {
			g.ledgerTTL = DefaultTerminalRecordTTL
		}
	}
	if g.admittedGroups == nil {
		g.admittedGroups = make(map[string]uint64)
	}
	if g.closedGroups == nil {
		g.closedGroups = make(map[string]uint64)
	}
	if g.closedGroupLimit <= 0 {
		g.closedGroupLimit = g.cfg.MaxTerminalEntries
		if g.closedGroupLimit <= 0 {
			g.closedGroupLimit = DefaultMaxTerminalEntries
		}
	}
	if g.closedGroupByteLimit <= 0 {
		g.closedGroupByteLimit = g.cfg.MaxTerminalBytes
		if g.closedGroupByteLimit <= 0 {
			g.closedGroupByteLimit = DefaultMaxTerminalBytes
		}
	}
	return nil
}

func (g *Gateway) claimGroupLocked(groupID string, groupEpoch uint64) error {
	if closedEpoch, ok := g.closedGroups[groupID]; ok && groupEpoch <= closedEpoch {
		return fmt.Errorf("%w: execution group %q epoch %d is already closed", protocol.ErrDuplicate, groupID, groupEpoch)
	}
	if admittedEpoch, ok := g.admittedGroups[groupID]; ok {
		return fmt.Errorf("%w: execution group %q epoch %d is already admitted", protocol.ErrDuplicate, groupID, admittedEpoch)
	}
	if _, exists := g.closedGroups[groupID]; !exists {
		// Reserve the future tombstone while the group is active. Checking only
		// closedGroups admits an unbounded number of concurrent groups between
		// two completions and can overflow the bounded fence ledger when they
		// drain together.
		if len(g.closedGroups)+len(g.admittedGroups) >= g.closedGroupLimit ||
			g.closedGroupBytes+g.reservedGroupBytes+int64(len(groupID)+8) > g.closedGroupByteLimit {
			return fmt.Errorf("%w: Python UDF closed-group fence is full", protocol.ErrLedgerFull)
		}
		g.reservedGroupBytes += int64(len(groupID) + 8)
	}
	g.admittedGroups[groupID] = groupEpoch
	return nil
}

func (g *Gateway) releaseGroupClaim(groupID string, groupEpoch uint64) {
	g.admissionMu.Lock()
	if admittedEpoch, ok := g.admittedGroups[groupID]; ok && admittedEpoch == groupEpoch {
		delete(g.admittedGroups, groupID)
		if _, closed := g.closedGroups[groupID]; !closed {
			g.reservedGroupBytes -= int64(len(groupID) + 8)
		}
	}
	g.admissionMu.Unlock()
}

func (g *Gateway) closeGroupEpoch(groupID string, groupEpoch uint64) {
	g.admissionMu.Lock()
	if admittedEpoch, ok := g.admittedGroups[groupID]; ok && admittedEpoch == groupEpoch {
		delete(g.admittedGroups, groupID)
		if _, closed := g.closedGroups[groupID]; !closed {
			g.reservedGroupBytes -= int64(len(groupID) + 8)
		}
	}
	if closedEpoch := g.closedGroups[groupID]; groupEpoch > closedEpoch {
		g.closedGroups[groupID] = groupEpoch
		if closedEpoch == 0 {
			g.closedGroupBytes += int64(len(groupID) + 8)
		}
	}
	g.admissionMu.Unlock()
}

func (g *Gateway) epochClosed(groupID string, groupEpoch uint64) bool {
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	closedEpoch, ok := g.closedGroups[groupID]
	return ok && groupEpoch <= closedEpoch
}

func invocationLedgerKey(tuple protocol.FencingTuple) (string, error) {
	wire, err := json.Marshal(tuple)
	if err != nil {
		return "", fmt.Errorf("python udf: encode invocation identity: %w", err)
	}
	digest := sha256.Sum256(wire)
	return hex.EncodeToString(digest[:]), nil
}

func admissionReason(ctx context.Context, attempted bool, err error) protocol.CloseReason {
	if err == nil {
		return protocol.ReasonInputEOF
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		(ctx != nil && ctx.Err() != nil) {
		return protocol.ReasonCancel
	}
	if !attempted {
		return protocol.ReasonPartialOpenError
	}
	return protocol.ReasonFailure
}

func (a *invocationAdmission) finish(attempted bool, reason protocol.CloseReason) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return nil
	}

	// The admission owner is normally reached from one defer.  A release
	// callback can still fail after the terminal protocol state is frozen, so
	// perform one bounded retry here before returning.  The group and ledger
	// operations are idempotent; only the group release callback has meaningful
	// retry state.  If both attempts fail, leave `finished` false so a higher
	// level shutdown/recovery owner can call finish again without losing the
	// resources still owned by this admission.
	for attempt := 0; attempt < 2; attempt++ {
		var errs []error
		if attempted {
			if err := a.gateway.ledger.Complete(a.key); err != nil && !errors.Is(err, protocol.ErrUnknownIdentity) {
				errs = append(errs, err)
			}
		} else if err := a.gateway.ledger.Abandon(a.key); err != nil && !errors.Is(err, protocol.ErrUnknownIdentity) {
			errs = append(errs, err)
		}
		if a.memberOpen {
			if err := a.group.MemberTerminal(a.invocationID); err != nil && !errors.Is(err, protocol.ErrUnknownIdentity) {
				errs = append(errs, err)
			}
		}
		if err := a.group.Close(reason); err != nil {
			errs = append(errs, err)
		}
		if a.group.State() == protocol.GroupReleased {
			a.gateway.closeGroupEpoch(a.group.ID(), a.group.Epoch())
		}
		if err := errors.Join(errs...); err == nil {
			a.finished = true
			return nil
		} else if attempt == 1 {
			return err
		}
	}
	return nil
}

type capabilityRequest struct {
	ProtocolVersion int `json:"protocol_version"`
}

// definitionValidationPayload is a create-time action payload. It is not a
// plan or an invocation envelope: it carries the source only after the
// Gateway has resolved the exact account-scoped artifact digest.
type definitionValidationPayload struct {
	AccountID               uint64           `json:"account_id"`
	Handler                 string           `json:"handler"`
	Source                  string           `json:"source"`
	Mode                    string           `json:"mode"`
	NullPolicy              string           `json:"null_policy"`
	ABIContract             string           `json:"abi_contract"`
	AdapterVersion          string           `json:"adapter_version"`
	SDKVersion              string           `json:"sdk_version"`
	DefinitionSchemaVersion int              `json:"definition_schema_version"`
	ArtifactDigest          string           `json:"artifact_digest"`
	EnvironmentDigest       string           `json:"environment_digest"`
	DefinitionFingerprint   string           `json:"definition_fingerprint"`
	Args                    []TypeDescriptor `json:"args"`
	Return                  TypeDescriptor   `json:"return"`
}

type definitionValidationResponse struct {
	Status                string `json:"status"`
	Reason                string `json:"reason,omitempty"`
	ArtifactDigest        string `json:"artifact_digest,omitempty"`
	DefinitionFingerprint string `json:"definition_fingerprint,omitempty"`
}

type capabilityResponse struct {
	ProtocolVersion            int      `json:"protocol_version"`
	ABIContract                string   `json:"abi_contract"`
	AdapterVersion             string   `json:"adapter_version"`
	SDKVersion                 string   `json:"sdk_version"`
	DefinitionSchemaVersion    int      `json:"definition_schema_version"`
	PlanContractVersion        int      `json:"plan_contract_version"`
	TypeDescriptorContract     string   `json:"type_descriptor_contract"`
	TimezoneDatabaseVersion    string   `json:"timezone_database_version"`
	Modes                      []string `json:"modes"`
	NullPolicies               []string `json:"null_policies"`
	WindowBatches              int      `json:"window_batches"`
	CumulativeAck              bool     `json:"cumulative_ack"`
	MaxExecutionFrameBytes     int64    `json:"max_execution_frame_bytes"`
	MaxHandlerProcesses        int      `json:"max_handler_processes"`
	MaxAccountHandlerProcesses int      `json:"max_account_handler_processes"`
	MaxOwnerHandlerProcesses   int      `json:"max_owner_handler_processes"`
	LeaseEpoch                 uint64   `json:"lease_epoch"`
}

// ensureCapabilities is the feature-level readiness gate. It runs once per
// Gateway connection before OpenInvocation, and it caches only a successful
// match. A worker with an unknown or different contract therefore cannot run
// user code, while ordinary SQL remains independent of this client.
func (g *Gateway) ensureCapabilities(ctx context.Context, client flight.FlightServiceClient) error {
	g.capabilityMu.Lock()
	defer g.capabilityMu.Unlock()
	g.mu.Lock()
	ready := g.capabilityReady
	closed := g.closed
	g.mu.Unlock()
	if closed {
		return errGatewayClosed
	}
	if ready {
		g.mu.Lock()
		leaseEpoch := g.workerLeaseEpoch
		g.mu.Unlock()
		if leaseEpoch != 0 {
			return nil
		}
		// A partially initialized Gateway must not treat readiness as a valid
		// worker contract. Fall through and establish the complete capability
		// record again.
	}
	request, err := json.Marshal(capabilityRequest{ProtocolVersion: protocol.Version})
	if err != nil {
		return err
	}
	actionCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	stream, err := client.DoAction(actionCtx, &flight.Action{Type: "GetPythonCapabilities", Body: request})
	if err != nil {
		return fmt.Errorf("python udf: capability handshake: %w", err)
	}
	result, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("python udf: capability handshake response: %w", err)
	}
	if result == nil {
		return fmt.Errorf("python udf: empty capability handshake response")
	}
	var response capabilityResponse
	decoder := json.NewDecoder(bytes.NewReader(result.Body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return fmt.Errorf("python udf: decode capability handshake: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("python udf: capability handshake has trailing JSON")
	}
	if err := validateCapabilities(response); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("python udf: capability handshake returned multiple results")
		}
		return fmt.Errorf("python udf: capability handshake stream: %w", err)
	}
	g.mu.Lock()
	if !g.closed {
		g.capabilityReady = true
		g.workerLeaseEpoch = response.LeaseEpoch
	}
	g.mu.Unlock()
	return nil
}

// invalidateCapabilities is called after a post-handshake execution error.
// In particular, a restarted worker keeps the same endpoint but advertises a
// new instance lease epoch; clearing the cached record makes the next
// invocation perform the handshake again. This never retries the failed
// invocation or changes its terminal outcome.
func (g *Gateway) invalidateCapabilities() {
	g.capabilityMu.Lock()
	defer g.capabilityMu.Unlock()
	g.mu.Lock()
	g.capabilityReady = false
	g.workerLeaseEpoch = 0
	g.mu.Unlock()
}

func validateCapabilities(response capabilityResponse) error {
	if response.ProtocolVersion != protocol.Version ||
		response.ABIContract != udf.PythonABIContract ||
		response.AdapterVersion != udf.PythonAdapterVersion ||
		response.SDKVersion != udf.PythonSDKVersion ||
		response.DefinitionSchemaVersion != udf.PythonDefinitionSchemaVersion ||
		response.PlanContractVersion != udf.PythonPlanContractVersion ||
		response.TypeDescriptorContract != udf.PythonTypeDescriptorContract ||
		response.TimezoneDatabaseVersion != currentTimezoneDatabaseVersion() ||
		response.WindowBatches != 1 || response.CumulativeAck ||
		response.MaxExecutionFrameBytes != 1<<30 ||
		response.MaxHandlerProcesses != DefaultWorkerMaxHandlerProcesses ||
		response.MaxAccountHandlerProcesses != DefaultWorkerMaxAccountHandlers ||
		response.MaxOwnerHandlerProcesses != DefaultWorkerMaxOwnerHandlers ||
		response.LeaseEpoch == 0 {
		return fmt.Errorf("python udf: worker capability contract does not match the current contract")
	}
	if !sameStrings(response.Modes, []string{ModeScalar, ModeVector}) ||
		!sameStrings(response.NullPolicies, []string{NullCallHandler, NullReturnNull}) {
		return fmt.Errorf("python udf: worker capability set does not match the current contract")
	}
	return nil
}

func currentTimezoneDatabaseVersion() string {
	version, err := udf.TimezoneDatabaseVersion()
	if err != nil {
		return ""
	}
	return version
}

func sameStrings(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func encodeInputBatch(inputs []*vector.Vector, args []types.Type, start, remaining, maxBytes, maxRows int64) (encodedRecordBatch, error) {
	encoder, err := newInputBatchEncoder(inputs, args)
	if err != nil {
		return encodedRecordBatch{}, err
	}
	return encodeInputBatchWithEncoder(encoder, start, remaining, maxBytes, maxRows)
}

func encodeInputBatchWithEncoder(encoder *inputBatchEncoder, start, remaining, maxBytes, maxRows int64) (encodedRecordBatch, error) {
	if start < 0 || remaining <= 0 {
		return encodedRecordBatch{}, fmt.Errorf("invalid Python UDF input batch range")
	}
	if maxRows <= 0 {
		return encodedRecordBatch{}, fmt.Errorf("invalid Python UDF max batch rows")
	}
	if remaining > maxRows {
		remaining = maxRows
	}
	// Fixed-width input records can be materialized once at the bounded row
	// limit and sliced for each size probe. This removes repeated Arrow builder
	// work without assuming anything about variable-length SQL values. The
	// slice keeps the same backing buffers, and every candidate is still encoded
	// and checked against maxBytes before publication.
	if encoder == nil {
		return encodedRecordBatch{}, fmt.Errorf("invalid Python UDF input encoder")
	}
	if encoder.fixedWidth {
		record, _, err := encoder.build(int(start), int(remaining))
		if err != nil {
			return encodedRecordBatch{}, err
		}
		defer record.Release()
		tryEncode := func(rows int64) ([]ArrowFrame, error) {
			candidate := record.NewSlice(0, rows)
			defer candidate.Release()
			return encoder.encode(candidate, maxBytes)
		}
		// The full fixed-width record is already materialized under the
		// invocation's bounded input range. Try publishing it once before the
		// size search; the normal SQL batch fits in the limit and avoids a
		// logarithmic series of identical IPC encodes.
		return chooseInputBatchWithFullCandidate(remaining, maxBytes, tryEncode, true)
	}
	tryEncode := func(rows int64) ([]ArrowFrame, error) {
		record, _, err := encoder.build(int(start), int(rows))
		if err != nil {
			return nil, err
		}
		frames, encodeErr := encoder.encode(record, maxBytes)
		record.Release()
		return frames, encodeErr
	}
	return chooseInputBatchWithFullCandidate(
		remaining, maxBytes, tryEncode,
		encoder.canBuildFullBatch(start, remaining, maxBytes),
	)
}

func chooseInputBatch(remaining, maxBytes int64, tryEncode func(int64) ([]ArrowFrame, error)) (encodedRecordBatch, error) {
	return chooseInputBatchWithFullCandidate(remaining, maxBytes, tryEncode, false)
}

func chooseInputBatchWithFullCandidate(remaining, maxBytes int64, tryEncode func(int64) ([]ArrowFrame, error), tryFull bool) (encodedRecordBatch, error) {
	if tryFull {
		frames, err := tryEncode(remaining)
		if err == nil {
			return encodedRecordBatch{Frames: frames, Rows: remaining}, nil
		}
		if !errors.Is(err, errArrowBatchTooLarge) {
			return encodedRecordBatch{}, err
		}
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
	client flight.FlightServiceClient,
	stream flight.FlightService_DoExchangeClient,
	tuple protocol.FencingTuple,
	sequenceNumber uint64,
	expectedRows int64,
	schemaFrame **ArrowFrame,
	schemaReady *bool,
	returnDescriptor TypeDescriptor,
	result vector.FunctionResultWrapper,
	mp *mpool.MPool,
	sequence *protocol.Sequence,
) (int, error) {
	inputConsumed := false
	for {
		data, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return 0, fmt.Errorf("python udf: exchange ended before result sequence %d", sequenceNumber)
			}
			return 0, fmt.Errorf("python udf: receive result: %w", err)
		}
		if data == nil || (len(data.AppMetadata) == 0 && len(data.DataHeader) == 0) {
			return 0, fmt.Errorf("python udf: empty Flight result")
		}
		if len(data.DataHeader) > 0 {
			if *schemaFrame == nil {
				if len(data.DataBody) != 0 {
					return 0, fmt.Errorf("python udf: result schema contains a body")
				}
				*schemaFrame = &ArrowFrame{Header: append([]byte(nil), data.DataHeader...)}
				if len(data.AppMetadata) != 0 {
					if err := validateResultSchemaMetadata(data.AppMetadata, tuple); err != nil {
						return 0, err
					}
					*schemaReady = true
				}
				continue
			}
			if !*schemaReady {
				return 0, fmt.Errorf("python udf: result schema metadata was not received")
			}
			control, err := protocol.UnmarshalControl(data.AppMetadata)
			if err != nil {
				return 0, err
			}
			if control.Tuple != tuple || control.Kind != "ResultBatch" || control.Sequence != sequenceNumber {
				return 0, fmt.Errorf("python udf: unexpected result sequence %d", control.Sequence)
			}
			if !inputConsumed {
				return 0, fmt.Errorf("python udf: result sequence %d arrived before InputConsumed", sequenceNumber)
			}
			if err := sequence.AcceptResult(control.Sequence); err != nil {
				return 0, err
			}
			snapshot, err := protocol.FreezeOutput(data.DataBody, g.cfg.MaxBatchBytes)
			if err != nil {
				return 0, err
			}
			// FreezeOutput already stores the digest of its private copy. An
			// empty expectedDigest still makes Validate rehash that same
			// backing and detect any trusted-boundary mutation, without
			// allocating the hexadecimal digest string twice on every batch.
			if err := snapshot.Validate(len(data.DataBody), ""); err != nil {
				return 0, err
			}
			// DecodeRecordBatch consumes the result header synchronously before
			// the next Recv.  The schema header is retained across batches, but
			// this per-batch header has no cross-call lifetime, so keep the
			// Flight-owned slice and avoid a redundant allocation/copy.
			decoded, err := DecodeRecordBatch(**schemaFrame, ArrowFrame{Header: data.DataHeader, Body: snapshot.TrustedBytes()}, g.cfg.MaxBatchBytes)
			if err != nil {
				return 0, fmt.Errorf("python udf: decode result: %w", err)
			}
			decodedRows := decoded.NumRows()
			if decoded.NumCols() != 1 || decodedRows <= 0 || decodedRows != expectedRows {
				decoded.Release()
				return 0, fmt.Errorf("python udf: result batch row count %d, expected %d", decodedRows, expectedRows)
			}
			if decoded.Schema().Field(0).Name != "result" {
				decoded.Release()
				return 0, fmt.Errorf("python udf: result field has unexpected name %q", decoded.Schema().Field(0).Name)
			}
			if err := returnDescriptor.ValidateField(decoded.Schema().Field(0)); err != nil {
				decoded.Release()
				return 0, err
			}
			if err := AppendArrowResult(returnDescriptor, decoded.Column(0), result, mp); err != nil {
				decoded.Release()
				return 0, err
			}
			if limit := g.cfg.MaxInvocationResultBytes; limit > 0 && result.GetResultVector() != nil && int64(result.GetResultVector().Size()) > limit {
				decoded.Release()
				return 0, fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF invocation result exceeds %d bytes", limit)
			}
			decoded.Release()
			if err := g.action(ctx, client, "AcknowledgeResults", protocol.Control{Kind: "AcknowledgeResults", Tuple: tuple, AckSequence: control.Sequence}); err != nil {
				return 0, err
			}
			if err := sequence.AcknowledgeResults(control.Sequence); err != nil {
				return 0, err
			}
			return int(expectedRows), nil
		}
		if len(data.DataBody) != 0 {
			return 0, fmt.Errorf("python udf: control frame contains an Arrow body")
		}
		control, err := protocol.UnmarshalControl(data.AppMetadata)
		if err != nil {
			return 0, err
		}
		if control.Tuple != tuple {
			return 0, fmt.Errorf("python udf: result tuple changed")
		}
		switch control.Kind {
		case "ResultSchema":
			if *schemaFrame == nil || *schemaReady {
				return 0, fmt.Errorf("python udf: invalid result schema metadata")
			}
			*schemaReady = true
			continue
		case "InputConsumed":
			if control.Sequence != sequenceNumber {
				return 0, fmt.Errorf("python udf: input sequence %d was consumed while waiting for result %d", control.Sequence, sequenceNumber)
			}
			if control.ReleasedBatches != 1 || control.ReleasedBytes <= 0 || control.ReleasedBytes > g.cfg.MaxBatchBytes {
				return 0, fmt.Errorf("python udf: input sequence %d released invalid bytes/batches (%d/%d)", sequenceNumber, control.ReleasedBytes, control.ReleasedBatches)
			}
			if inputConsumed {
				return 0, fmt.Errorf("python udf: input sequence %d was consumed more than once", sequenceNumber)
			}
			inputConsumed = true
			continue
		case "Error":
			return 0, fmt.Errorf("python udf: worker error: %s", control.Reason)
		case "Finish":
			return 0, fmt.Errorf("python udf: Finish arrived before result sequence %d", sequenceNumber)
		default:
			return 0, fmt.Errorf("python udf: unexpected control %q", control.Kind)
		}
	}
}

type executionBudget struct {
	rows           int64
	maxRows        int64
	returnType     types.Type
	maxResultBytes int64
}

func validateInvocationBudget(budget executionBudget) error {
	if budget.rows < 0 {
		return fmt.Errorf("python udf: invocation row count is negative")
	}
	if budget.maxRows > 0 && budget.rows > budget.maxRows {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF invocation has %d rows, limit is %d", budget.rows, budget.maxRows)
	}
	if budget.maxResultBytes <= 0 || budget.rows == 0 {
		return nil
	}
	// PreExtendAndReset allocates the fixed-width payload and the validity
	// domain before the first result arrives.  Check that unavoidable portion
	// before OpenInvocation; variable-width payloads are checked again after
	// each validated append because their area size depends on user values.
	validityBytes := (budget.rows-1)/8 + 1
	if validityBytes >= budget.maxResultBytes {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF invocation result reservation exceeds %d bytes", budget.maxResultBytes)
	}
	perRow := int64(budget.returnType.TypeSize())
	if perRow > 0 && budget.rows > (budget.maxResultBytes-validityBytes)/perRow {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF invocation result reservation exceeds %d bytes", budget.maxResultBytes)
	}
	return nil
}

func validateResultSchemaMetadata(metadata []byte, tuple protocol.FencingTuple) error {
	control, err := protocol.UnmarshalControl(metadata)
	if err != nil || control.Tuple != tuple || control.Kind != "ResultSchema" {
		return fmt.Errorf("python udf: invalid result schema metadata")
	}
	return nil
}

func (g *Gateway) receiveFinish(
	ctx context.Context,
	client flight.FlightServiceClient,
	stream flight.FlightService_DoExchangeClient,
	tuple protocol.FencingTuple,
	expectedRows, rows int,
	sequence *protocol.Sequence,
) error {
	for {
		data, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("python udf: exchange ended before Finish")
			}
			return fmt.Errorf("python udf: receive Finish: %w", err)
		}
		if data == nil || len(data.DataHeader) > 0 || len(data.DataBody) > 0 {
			return fmt.Errorf("python udf: unexpected Arrow data while finishing")
		}
		control, err := protocol.UnmarshalControl(data.AppMetadata)
		if err != nil {
			return err
		}
		if control.Tuple != tuple {
			return fmt.Errorf("python udf: Finish tuple changed")
		}
		switch control.Kind {
		case "InputConsumed", "ResultSchema":
			return fmt.Errorf("python udf: %s arrived after result stream was drained", control.Kind)
		case "Error":
			return fmt.Errorf("python udf: worker error: %s", control.Reason)
		case "Finish":
			lastInput, lastResult, acked, inputEnded := sequence.State()
			if control.Status != statusOK || control.FinishID == "" || rows != expectedRows ||
				!inputEnded || control.LastSequence != lastInput || lastResult != lastInput || acked != lastInput || !sequence.ReadyToFinish() {
				return fmt.Errorf("python udf: invalid Finish status=%q rows=%d", control.Status, rows)
			}
			if control.LastResultSequence != lastResult {
				return fmt.Errorf("python udf: invalid Finish result sequence %d, expected %d", control.LastResultSequence, lastResult)
			}
			return g.acknowledgeFinish(ctx, client, "AcknowledgeFinish", protocol.Control{Kind: "AcknowledgeFinish", Tuple: tuple, FinishID: control.FinishID})
		default:
			return fmt.Errorf("python udf: unexpected control %q", control.Kind)
		}
	}
}

// acknowledgeFinish retries only the idempotent terminal ACK. A lost ACK
// request or response must not replay any STARTED input or handler work.
// Once the request budget is exhausted the caller preserves the distinction
// between an accepted terminal result and a locally unconfirmed Finish.
func (g *Gateway) acknowledgeFinish(ctx context.Context, client flight.FlightServiceClient, kind string, finish protocol.Control) error {
	if client == nil {
		return fmt.Errorf("FINISH_UNCONFIRMED: finish ACK client is unavailable")
	}
	firstErr := g.action(ctx, client, kind, finish)
	if firstErr == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("FINISH_UNCONFIRMED: finish ACK budget expired: %w", firstErr)
	}
	if retryErr := g.action(ctx, client, kind, finish); retryErr == nil {
		return nil
	} else {
		return fmt.Errorf("FINISH_UNCONFIRMED: finish ACK was not confirmed after idempotent retry: %v; retry: %w", firstErr, retryErr)
	}
}

func validateInvocation(invocation *udf.Invocation) error {
	if err := validateInvocationHeader(invocation); err != nil {
		return err
	}
	if invocation.Source == "" {
		return fmt.Errorf("python udf: handler and source are required")
	}
	if invocation.ArtifactDigest != udf.PythonInlineArtifactDigest(invocation.Handler, invocation.Source) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python artifact digest does not match the immutable source")
	}
	return nil
}

// validateInvocationHeader checks every field that is independent of the
// artifact bytes.  The CN must run this before an account-scoped artifact
// lookup; otherwise a malformed plan could make the resolver inspect an
// arbitrary tenant path before its FunctionRef and fencing tuple are known to
// be valid.
func validateInvocationHeader(invocation *udf.Invocation) error {
	if invocation == nil {
		return fmt.Errorf("python udf: nil invocation")
	}
	if invocation.Language != "" && invocation.Language != udf.LanguagePython {
		return fmt.Errorf("python udf: unsupported language %q", invocation.Language)
	}
	if invocation.Handler == "" {
		return fmt.Errorf("python udf: handler is required")
	}
	if strings.Contains(invocation.Handler, ":") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python external handler import requires an immutable artifact catalog")
	}
	if invocation.Length < 0 || len(invocation.Args) != len(invocation.Inputs) {
		return fmt.Errorf("python udf: invalid input shape")
	}
	if invocation.Mode != ModeScalar && invocation.Mode != ModeVector {
		return fmt.Errorf("python udf: unsupported mode %q", invocation.Mode)
	}
	if invocation.CallsiteID == "" || len(invocation.CallsiteID) > 256 || strings.ContainsAny(invocation.CallsiteID, "\r\n") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invocation has no valid callsite id")
	}
	if !invocation.MayError || invocation.SecurityMode != "INVOKER" || invocation.Leakproof {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invocation semantic contract is not supported")
	}
	if invocation.StatementContext == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invocation has no statement context")
	}
	if err := invocation.StatementContext.Validate(); err != nil {
		return err
	}
	if invocation.SecurityFrame == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invocation has no security frame")
	}
	if err := invocation.SecurityFrame.Validate(); err != nil {
		return err
	}
	if invocation.SecurityFrame.Mode != invocation.SecurityMode {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invocation security mode does not match its frame")
	}
	if invocation.NullPolicy != NullCallHandler && invocation.NullPolicy != NullReturnNull {
		return fmt.Errorf("python udf: unsupported NULL policy %q", invocation.NullPolicy)
	}
	if invocation.ABIContract != udf.PythonABIContract || invocation.AdapterVersion != udf.PythonAdapterVersion {
		return fmt.Errorf("python udf: unsupported Python ABI contract %q/%q", invocation.ABIContract, invocation.AdapterVersion)
	}
	if invocation.SDKVersion != udf.PythonSDKVersion {
		return fmt.Errorf("python udf: unsupported Python SDK %q", invocation.SDKVersion)
	}
	if invocation.DefinitionSchemaVersion != udf.PythonDefinitionSchemaVersion {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported Python definition schema %d", invocation.DefinitionSchemaVersion)
	}
	if !udf.IsSHA256Digest(invocation.ArtifactDigest) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python artifact digest does not match the immutable source")
	}
	environmentDigest, err := udf.PythonEnvironmentDigest()
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python environment digest is unavailable: %w", err)
	}
	if invocation.EnvironmentDigest != environmentDigest {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python environment digest does not match the current worker contract")
	}
	if !udf.IsSHA256Digest(invocation.DefinitionFingerprint) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint is invalid")
	}
	args := make([]TypeDescriptor, len(invocation.Args))
	for index, typ := range invocation.Args {
		descriptor, err := NewTypeDescriptor(typ)
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python argument descriptor %d: %w", index, err)
		}
		args[index] = descriptor
	}
	returnDescriptor, err := NewTypeDescriptor(invocation.ReturnType)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python return descriptor: %w", err)
	}
	if err := validateInvocationInputs(invocation, args); err != nil {
		return err
	}
	expectedFingerprint, err := DefinitionFingerprint(
		invocation.DefinitionSchemaVersion,
		invocation.Handler, invocation.Mode, invocation.NullPolicy,
		invocation.ABIContract, invocation.AdapterVersion,
		invocation.ArtifactDigest, invocation.EnvironmentDigest, invocation.SDKVersion,
		args, returnDescriptor,
	)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint cannot be computed: %w", err)
	}
	if invocation.DefinitionFingerprint != expectedFingerprint {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint does not match the typed definition")
	}
	if err := invocation.FunctionRef.Validate(); err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: %w", err)
	}
	if invocation.FunctionRef.AccountID != invocation.Tuple.AccountID {
		return fmt.Errorf("python udf: FunctionRef account does not match the fencing tuple")
	}
	return invocation.Tuple.Validate()
}

func validateInvocationInputs(invocation *udf.Invocation, args []TypeDescriptor) error {
	if invocation == nil || len(invocation.Inputs) != len(args) {
		return fmt.Errorf("python udf: invalid input shape")
	}
	// An empty physical selection has no input backing to inspect.  Nil inputs
	// are valid in this case, but a vector supplied by a caller still has to
	// match the frozen descriptor.  Otherwise a zero-row invocation could
	// bypass the same type boundary enforced for non-empty execution.
	if invocation.Length == 0 {
		for index, input := range invocation.Inputs {
			if input == nil {
				continue
			}
			if input.GetType() == nil || !input.GetType().Eq(invocation.Args[index]) {
				return fmt.Errorf(
					"python udf: input column %d type does not match the frozen argument type",
					index,
				)
			}
		}
		return nil
	}
	for index, input := range invocation.Inputs {
		if input == nil || input.GetType() == nil || input.Length() == 0 {
			return fmt.Errorf("python udf: input column %d has no backing vector", index)
		}
		if !input.GetType().Eq(invocation.Args[index]) {
			return fmt.Errorf(
				"python udf: input column %d type %s does not match the frozen argument type %s",
				index,
				input.GetType().DescString(),
				invocation.Args[index].DescString(),
			)
		}
		if !input.IsConst() && input.Length() < invocation.Length {
			return fmt.Errorf(
				"python udf: input column %d has %d rows, expected at least %d",
				index,
				input.Length(),
				invocation.Length,
			)
		}
	}
	return nil
}

func (g *Gateway) action(ctx context.Context, client flight.FlightServiceClient, kind string, control protocol.Control) error {
	body, err := protocol.MarshalControl(control)
	if err != nil {
		return err
	}
	actionCtx, cancel := context.WithTimeout(ctx, g.cfg.RequestTimeout)
	defer cancel()
	stream, err := client.DoAction(actionCtx, &flight.Action{Type: kind, Body: body})
	if err != nil {
		return fmt.Errorf("python udf: %s: %w", kind, err)
	}
	result, err := stream.Recv()
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("python udf: empty %s response", kind)
	}
	ack, err := protocol.UnmarshalControl(result.Body)
	if err != nil {
		return err
	}
	if err := validateActionAck(kind, control, ack); err != nil {
		return err
	}
	if ack.Status != statusOK {
		return fmt.Errorf("python udf: %s rejected: %s", kind, ack.Reason)
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("python udf: %s returned multiple results", kind)
		}
		return err
	}
	return nil
}

func validateActionAck(kind string, request, ack protocol.Control) error {
	if ack.Tuple != request.Tuple || ack.Kind != "Ack" {
		return fmt.Errorf("python udf: %s returned an invalid ACK", kind)
	}
	switch kind {
	case "AcknowledgeResults":
		if ack.AckSequence != request.AckSequence {
			return fmt.Errorf("python udf: %s ACK sequence %d, expected %d", kind, ack.AckSequence, request.AckSequence)
		}
	case "AcknowledgeFinish":
		if ack.FinishID != request.FinishID {
			return fmt.Errorf("python udf: %s ACK finish ID does not match the request", kind)
		}
	default:
		return fmt.Errorf("python udf: unsupported action %q", kind)
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
