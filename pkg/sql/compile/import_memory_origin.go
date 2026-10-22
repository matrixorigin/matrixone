// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type statementMemoryControl struct {
	addr        string
	token       string
	unit        int64
	stream      morpc.Stream
	receive     chan morpc.Message
	commitLocal func(int64) error
	closeLocal  func(error)
	memory      *process.ImportMemory
}

type memoryCNManifest struct {
	id, addr, queryAddr string
	processID           string
	manifest            MemoryManifest
}

// The coordinator belongs to a process, not a CN service. Different queries
// may address different aliases of that same physical resource.
func memoryCNLess(left, right *memoryCNManifest) bool {
	if left.processID != right.processID {
		return left.processID < right.processID
	}
	return left.id < right.id
}

// memoryExecutionLocations follows the same remote fallback decision as
// RemoteRun before admission. No operator is prepared and no source is read.
func (c *Compile) memoryExecutionLocations() map[*Scope]string {
	result := make(map[*Scope]string)
	var walk func(*Scope, string)
	walk = func(s *Scope, parent string) {
		if s == nil {
			return
		}
		addr := parent
		if s.Magic == Remote && !s.ipAddrMatch(parent) && checkPipelineStandaloneExecutableAtRemote(s) {
			addr = s.NodeInfo.Addr
		}
		result[s] = addr
		for _, child := range s.PreScopes {
			walk(child, addr)
		}
	}
	for _, s := range c.scopes {
		walk(s, c.addr)
	}
	return result
}

func memoryStage(op vm.Operator, count int) (MemoryStageDescriptor, bool, error) {
	stage := MemoryStageDescriptor{Count: uint32(count)}
	switch value := op.(type) {
	case *external.External:
		if !value.UsesMemoryInput() {
			return stage, false, nil
		}
		stage.Role = MemoryStageCSV
		stage.Schema = make([]types.Type, len(value.Es.Cols))
		for i, col := range value.Es.Cols {
			stage.Schema[i] = plan2.MakeTypeByPlan2Type(col.Typ)
		}
		stage.RetainedFields = max(0, int(value.Es.ColumnListLen)+2)
		for _, attr := range value.Es.Attrs {
			stage.RetainedFields = max(stage.RetainedFields, int(attr.ColFieldIndex)+1)
		}
	case *multi_update.MultiUpdate:
		stage.Role = MemoryStageOutput
		stage.TableCount = len(value.MultiUpdateCtx)
		for _, table := range value.MultiUpdateCtx {
			stage.Projections = append(stage.Projections, tableMemorySchema(table.TableDef))
		}
	case *insert.Insert:
		if !value.ToWriteS3 || value.InsertCtx == nil {
			return stage, false, nil
		}
		stage.Role = MemoryStageOutput
		stage.TableCount = 1
		stage.Schema = tableMemorySchema(value.InsertCtx.TableDef)
	case *hashjoin.HashJoin:
		stage.Role = MemoryStageJoin
		stage.LeftTypes = value.LeftTypes
		stage.RightTypes = value.RightTypes
		stage.ResultCols = value.ResultCols
		if value.NonEqCond != nil {
			var err error
			stage.NonEqExpression, err = value.NonEqCond.Marshal()
			if err != nil {
				return stage, false, err
			}
		}
		stage.KeyExpressions = make([][][]byte, len(value.EqConds))
		for side, expressions := range value.EqConds {
			for _, expr := range expressions {
				if expr == nil {
					return stage, false, errors.New("nil join memory expression")
				}
				encoded, err := expr.Marshal()
				if err != nil {
					return stage, false, err
				}
				stage.KeyExpressions[side] = append(stage.KeyExpressions[side], encoded)
			}
		}
	default:
		return stage, false, nil
	}
	return stage, true, nil
}

func (c *Compile) collectMemoryManifests(target int64) (map[string]*memoryCNManifest, map[*Scope]string, error) {
	locations := c.memoryExecutionLocations()
	manifests := make(map[string]*memoryCNManifest)
	// Include the origin even if it has no controlled stage: all queries acquire
	// any resources there in exactly the same global CN ordering.
	manifests[c.addr] = &memoryCNManifest{addr: c.addr, id: c.proc.GetService(), manifest: MemoryManifest{TargetUnit: target, Parallel: c.ncpu}}
	var walk func(*Scope) error
	walk = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		addr := locations[scope]
		entry := manifests[addr]
		if entry == nil {
			entry = &memoryCNManifest{addr: addr, id: scope.NodeInfo.Id, manifest: MemoryManifest{TargetUnit: target, Parallel: c.ncpu}}
			manifests[addr] = entry
		}
		if entry.id == "" {
			entry.id = scope.NodeInfo.Id
		}
		if err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			count := max(1, scope.NodeInfo.Mcpu)
			if uint64(count) > uint64(^uint32(0)) {
				return errors.New("import parallelism overflows manifest")
			}
			stage, ok, err := memoryStage(op, count)
			if err != nil {
				return err
			}
			if ok {
				entry.manifest.Stages = append(entry.manifest.Stages, stage)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, child := range scope.PreScopes {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, scope := range c.scopes {
		if err := walk(scope); err != nil {
			return nil, nil, err
		}
	}
	return manifests, locations, nil
}

func (c *Compile) resolveMemoryCNs(manifests map[string]*memoryCNManifest) ([]*memoryCNManifest, error) {
	rt := runtime.ServiceRuntime(c.proc.GetService())
	value, ok := rt.GetGlobalVariables(runtime.ClusterService)
	if !ok {
		return nil, errors.New("memory admission requires the CN service directory")
	}
	cluster, ok := value.(clusterservice.MOCluster)
	if !ok {
		return nil, errors.New("invalid CN service directory")
	}
	cluster.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		for _, entry := range manifests {
			if entry.addr == cn.PipelineServiceAddress || entry.addr == c.addr && cn.ServiceID == c.proc.GetService() {
				entry.id = cn.ServiceID
				entry.queryAddr = cn.QueryAddress
			}
		}
		return true
	})
	result := make([]*memoryCNManifest, 0, len(manifests))
	seen := make(map[string]bool)
	for _, entry := range manifests {
		if entry.id == "" || entry.queryAddr == "" {
			return nil, fmt.Errorf("CN metadata unavailable for memory admission at %s", entry.addr)
		}
		if seen[entry.id] {
			return nil, fmt.Errorf("multiple pipeline addresses for CN %s", entry.id)
		}
		seen[entry.id] = true
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return memoryCNLess(result[i], result[j]) })
	return result, nil
}

func (c *Compile) memoryCapabilities(ctx context.Context, entries []*memoryCNManifest) (bool, error) {
	client := c.proc.GetQueryClient()
	if client == nil {
		return false, errors.New("query client unavailable for memory capability discovery")
	}
	supported := true
	processes := make(map[string]bool, len(entries))
	for _, entry := range entries {
		entry.processID = ""
		req := client.NewRequest(query.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &query.GetProtocolVersionRequest{}
		response, err := client.SendMessage(ctx, entry.queryAddr, req)
		if err != nil {
			return false, err
		}
		if response == nil {
			return false, errors.New("empty memory capability response")
		}
		err = response.UnwrapError()
		capable := response.GetProtocolVersion != nil && response.GetProtocolVersion.MemoryAdmission
		if capable {
			entry.processID = response.GetProtocolVersion.MemoryAdmissionProcessId
			// Earlier admission-capable peers lack physical resource identity.
			// Multiple services in one process would recursively reserve the same
			// coordinator, so the complete statement keeps its legacy path.
			capable = entry.processID != "" && !processes[entry.processID]
			processes[entry.processID] = true
		}
		client.Release(response)
		if err != nil {
			return false, err
		}
		supported = supported && capable
	}
	sort.Slice(entries, func(i, j int) bool { return memoryCNLess(entries[i], entries[j]) })
	return supported, nil
}

func (h *statementMemoryControl) request(ctx context.Context, request MemoryControlRequest) (MemoryControlResponse, error) {
	var empty MemoryControlResponse
	data, err := EncodeMemoryControlRequest(request)
	if err != nil {
		return empty, err
	}
	msg := cnclient.AcquireMessage()
	msg.SetID(h.stream.ID())
	msg.Cmd = pipeline.Method_StopSending
	msg.Data = data
	// morpc requires a deadline. The control stream is independent of normal scope
	// cancellation; each operation still observes its caller's cancellation.
	sendCtx, cancel := context.WithTimeout(ctx, MaxRpcTime)
	defer cancel()
	if err = h.stream.Send(sendCtx, msg); err != nil {
		return empty, err
	}
	select {
	case <-sendCtx.Done():
		return empty, context.Cause(sendCtx)
	case raw, ok := <-h.receive:
		if !ok || raw == nil {
			return empty, errors.New("memory control connection closed")
		}
		reply, ok := raw.(*pipeline.Message)
		if !ok {
			return empty, errors.New("invalid memory control message")
		}
		if rpcErr, reported := reply.TryToGetMoErr(); reported {
			return empty, rpcErr
		}
		response, err := DecodeMemoryControlResponse(reply.Data)
		if err != nil {
			return empty, err
		}
		if response.RetryParallel > 0 {
			return response, &importDOPRetryError{response.RetryParallel}
		}
		if response.Error != "" {
			return response, errors.New(response.Error)
		}
		return response, nil
	}
}

func openRemoteMemory(ctx context.Context, serviceID, addr string, manifest MemoryManifest, expectedProcessID string) (*statementMemoryControl, error) {
	if expectedProcessID == "" {
		return nil, errors.New("memory admission requires the expected process identity")
	}
	client := cnclient.GetPipelineClient(serviceID)
	if client == nil {
		return nil, errors.New("pipeline client unavailable for memory admission")
	}
	// The creation context only bounds backend acquisition. The resulting
	// stream owns an independent lifetime until explicit Close on every terminal path.
	controlClient, capable := client.(cnclient.ControlStreamClient)
	if !capable {
		return nil, errors.New("pipeline client lacks an independent memory control transport")
	}
	stream, err := controlClient.NewControlStream(ctx, addr)
	if err != nil {
		return nil, err
	}
	h := &statementMemoryControl{stream: stream}
	success := false
	defer func() {
		if !success {
			_ = stream.Close(true)
		}
	}()
	h.receive, err = stream.Receive()
	if err != nil {
		return nil, err
	}
	response, err := h.request(ctx, MemoryControlRequest{Action: MemoryControlOpen, Manifest: &manifest, ExpectedProcessID: expectedProcessID})
	if err != nil {
		return nil, err
	}
	if response.Token == "" || response.UnitBytes <= 0 {
		return nil, errors.New("invalid memory admission lease")
	}
	h.token = response.Token
	h.unit = response.UnitBytes
	success = true
	return h, nil
}

func (h *statementMemoryControl) commit(ctx context.Context, unit int64) error {
	if unit <= 0 || unit > h.unit {
		return errors.New("memory commit cannot increase the admitted unit")
	}
	if h.commitLocal != nil {
		return h.commitLocal(unit)
	}
	if h.memory != nil {
		h.memory.UnitBytes = unit
		return nil
	}
	_, err := h.request(ctx, MemoryControlRequest{Action: MemoryControlCommit, Token: h.token, UnitBytes: unit})
	return err
}
func (h *statementMemoryControl) close(cause error) {
	if h.closeLocal != nil {
		h.closeLocal(cause)
		h.closeLocal = nil
		return
	}
	if h.stream == nil {
		return
	}
	// END must remain possible after query cancellation. Failure closes the locked
	// backend connection so SessionCtx owns cleanup even when END/ACK is lost.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	action := MemoryControlEnd
	if cause != nil {
		action = MemoryControlAbort
	}
	_, err := h.request(ctx, MemoryControlRequest{Action: action, Token: h.token})
	cancel()
	_ = h.stream.Close(err != nil)
	h.stream = nil
}

func closeMemoryControls(controls []*statementMemoryControl, cause error) {
	for i := len(controls) - 1; i >= 0; i-- {
		controls[i].close(cause)
	}
}

// beginStatementImportMemory acquires complete CN manifests in a common process
// incarnation UUID order. A retained process waits only for a later process;
// queries cannot introduce the reverse edge. Every partial owner is released
// before DOP recompilation or a terminal failure.
func (c *Compile) beginStatementImportMemory(ctx context.Context, t rscthrottler.RSCThrottler) error {
	c.clearImportMemoryTokens()
	if !c.supportsBoundedImportKeys() {
		return nil
	}
	target := rscthrottler.BatchBytes(t)
	manifests, locations, err := c.collectMemoryManifests(target)
	if err != nil {
		return err
	}
	// The source allocation table is currently proven for CSV/JSONLINE.
	// Ordinary table scans can produce wider rows and do not use that byte unit;
	// joins or writers alone must not opt an unrelated statement into this protocol.
	hasBoundedSource := false
	for _, entry := range manifests {
		for _, stage := range entry.manifest.Stages {
			hasBoundedSource = hasBoundedSource || stage.Role == MemoryStageCSV && stage.Count > 0
		}
	}
	if !hasBoundedSource {
		return nil
	}
	if len(manifests) == 1 {
		localManifest := manifests[c.addr].manifest
		localManifest.IncomingUnit = 0
		memory, err := admitMemoryManifest(ctx, t, localManifest)
		if err != nil {
			return err
		}
		if memory != nil {
			c.installImportMemory(memory)
		}
		return nil
	}
	entries, err := c.resolveMemoryCNs(manifests)
	if err != nil {
		return err
	}
	capable, err := c.memoryCapabilities(ctx, entries)
	if err != nil {
		return err
	}
	if !capable {
		for _, scope := range c.scopes {
			disableUncontrolledBroadcastSpill(scope)
		}
		return nil
	}
	controls, _, acquireErr := acquireMemoryControls(ctx, entries, func(ctx context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		if entry.id != c.proc.GetService() {
			return openRemoteMemory(ctx, c.proc.GetService(), entry.addr, entry.manifest, entry.processID)
		}
		return openLocalMemory(ctx, t, entry)
	})
	if acquireErr != nil {
		return acquireErr
	}
	success := false
	defer func() {
		if !success {
			closeMemoryControls(controls, err)
		}
	}()
	tokens := make(map[string]string)
	var local *process.ImportMemory
	for _, handle := range controls {
		tokens[handle.addr] = handle.token
		if handle.memory != nil {
			local = handle.memory
		}
	}
	if local != nil {
		shared, detach, attachErr := AttachStatementMemory(c.proc.GetService(), tokens[c.addr], c.proc.GetMessageBoard(), c.proc.Cancel)
		if attachErr != nil {
			err = attachErr
			return err
		}
		c.attachImportMemory(shared)
		c.importMemoryDetach = detach
	}
	for scope, addr := range locations {
		scope.MemoryToken = tokens[addr]
	}
	c.importMemoryControls = controls
	success = true
	return nil
}

func openLocalMemory(ctx context.Context, t rscthrottler.RSCThrottler, entry *memoryCNManifest) (*statementMemoryControl, error) {
	if entry.processID != runtime.ProcessInstanceID() {
		return nil, errors.New("memory admission process identity changed after capability discovery")
	}
	memory, err := admitMemoryManifest(ctx, t, entry.manifest)
	if err != nil {
		return nil, err
	}
	if memory == nil {
		return nil, errors.New("local memory manifest has no controlled stages")
	}
	token, closeOwner, err := RegisterStatementMemory(entry.id, memory)
	if err != nil {
		memory.WorkingSet.Close()
		return nil, err
	}
	return &statementMemoryControl{token: token, unit: memory.UnitBytes, memory: memory, closeLocal: closeOwner, commitLocal: func(unit int64) error { return CommitStatementMemory(entry.id, token, unit) }}, nil
}

// acquireMemoryControls is the single ordering point used by every origin.
// It deliberately owns partial cleanup, including commit failure, before the
// caller can recompile a lower-parallelism physical graph.
func acquireMemoryControls(ctx context.Context, entries []*memoryCNManifest, open func(context.Context, *memoryCNManifest) (*statementMemoryControl, error)) (controls []*statementMemoryControl, unit int64, err error) {
	ordered := append([]*memoryCNManifest(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return memoryCNLess(ordered[i], ordered[j]) })
	success := false
	defer func() {
		if !success {
			closeMemoryControls(controls, err)
			controls = nil
		}
	}()
	for _, entry := range ordered {
		if len(entry.manifest.Stages) == 0 {
			continue
		}
		handle, openErr := open(ctx, entry)
		if openErr != nil {
			return controls, 0, openErr
		}
		handle.addr = entry.addr
		controls = append(controls, handle)
		if unit == 0 {
			unit = handle.unit
		} else {
			unit = min(unit, handle.unit)
		}
	}
	for _, handle := range controls {
		if err = handle.commit(ctx, unit); err != nil {
			return controls, 0, err
		}
	}
	success = true
	return controls, unit, nil
}

func (c *Compile) clearImportMemoryTokens() {
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil {
			return
		}
		scope.MemoryToken = ""
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}

// Keep the complete statement on its existing path when any producer lacks a
// bounded lifetime contract. Partition wrappers hide independent writers, memory
// tables return retained raw data, and derived PreInsert columns still evaluate
// an entire batch without a separate input/scratch promise.
func (c *Compile) supportsBoundedImportKeys() bool {
	supported := true
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil || !supported {
			return
		}
		vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch value := op.(type) {
			case *insert.PartitionInsert, *multi_update.PartitionMultiUpdate:
				supported = false
			case *insert.Insert:
				if value.ToWriteS3 && value.InsertCtx != nil {
					_, memoryTable := value.InsertCtx.Engine.(*memoryengine.BindedEngine)
					supported = supported && !memoryTable
				}
			case *preinsert.PreInsert:
				supported = supported && value.CompPkeyExpr == nil && value.ClusterByExpr == nil
			case *hashbuild.HashBuild:
				supported = supported && hashbuild.SupportsBoundedSpillKeys(value.Conditions)
			case *hashjoin.HashJoin:
				if value.NonEqCond != nil {
					supported = supported && hashbuild.SupportsBoundedSpillKeys([]*plan.Expr{value.NonEqCond})
				}
				supported = supported && len(value.EqConds) == 2
				for _, side := range value.EqConds {
					supported = supported && hashbuild.SupportsBoundedSpillKeys(side)
				}
			}
			return nil
		})
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
	return supported
}
