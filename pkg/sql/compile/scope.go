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

package compile

import (
	"context"
	"fmt"
	"hash/crc32"
	goruntime "runtime"
	"runtime/debug"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func newScope(magic magicType) *Scope {
	s := reuse.Alloc[Scope](nil)
	s.Magic = magic
	return s
}

func ReleaseScopes(ss []*Scope) {
	for i := range ss {
		ss[i].release()
	}
}

func (s *Scope) withPlan(pn *plan.Plan) *Scope {
	s.Plan = pn
	return s
}

func (s *Scope) release() {
	if s == nil {
		return
	}
	for i := range s.PreScopes {
		s.PreScopes[i].release()
	}
	for i := range s.Instructions {
		s.Instructions[i].Arg.Release()
		s.Instructions[i].Arg = nil
	}
	reuse.Free[Scope](s, nil)
}

func (s *Scope) initDataSource(c *Compile) (err error) {
	if s.DataSource == nil {
		return nil
	}
	if s.DataSource.isConst {
		if s.DataSource.Bat != nil {
			return
		}
		bat, err := constructValueScanBatch(s.Proc.Ctx, c.proc, s.DataSource.node)
		if err != nil {
			return err
		}
		s.DataSource.Bat = bat
	} else {
		if s.DataSource.TableDef != nil {
			return nil
		}
		return c.compileTableScanDataSource(s)
	}
	return nil
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(c *Compile) (err error) {
	var p *pipeline.Pipeline
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			c.proc.Error(c.ctx, "panic in scope run",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
		if p != nil {
			p.Cleanup(s.Proc, err != nil, err)
		}
	}()

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	// DataSource == nil specify the empty scan
	if s.DataSource == nil {
		p = pipeline.New(0, nil, s.Instructions, s.Reg)
		if _, err = p.ConstRun(nil, s.Proc); err != nil {
			return err
		}
	} else {
		id := uint64(0)
		if s.DataSource.TableDef != nil {
			id = s.DataSource.TableDef.TblId
		}
		p = pipeline.New(id, s.DataSource.Attributes, s.Instructions, s.Reg)
		if s.DataSource.isConst {
			_, err = p.ConstRun(s.DataSource.Bat, s.Proc)
		} else {
			var tag int32
			if s.DataSource.node != nil && len(s.DataSource.node.RecvMsgList) > 0 {
				tag = s.DataSource.node.RecvMsgList[0].MsgTag
			}
			_, err = p.Run(s.DataSource.R, tag, s.Proc)
		}
	}

	select {
	case <-s.Proc.Ctx.Done():
		err = nil
	default:
	}
	return err
}

func (s *Scope) SetContextRecursively(ctx context.Context) {
	if s.Proc == nil {
		return
	}
	newCtx := s.Proc.ResetContextFromParent(ctx)
	for _, scope := range s.PreScopes {
		scope.SetContextRecursively(newCtx)
	}
}

func (s *Scope) InitAllDataSource(c *Compile) error {
	err := s.initDataSource(c)
	if err != nil {
		return err
	}
	for _, scope := range s.PreScopes {
		err := scope.InitAllDataSource(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) SetOperatorInfoRecursively(cb func() int32) {
	for i := 0; i < len(s.Instructions); i++ {
		s.Instructions[i].CnAddr = s.NodeInfo.Addr
		s.Instructions[i].OperatorID = cb()
		s.Instructions[i].ParallelID = 0
		s.Instructions[i].MaxParallel = 1
	}

	for _, scope := range s.PreScopes {
		scope.SetOperatorInfoRecursively(cb)
	}
}

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(c *Compile) error {
	var wg sync.WaitGroup

	errChan := make(chan error, len(s.PreScopes))
	for i := range s.PreScopes {
		wg.Add(1)
		scope := s.PreScopes[i]
		errSubmit := ants.Submit(func() {
			defer func() {
				wg.Done()
			}()

			switch scope.Magic {
			case Normal:
				errChan <- scope.Run(c)
			case Merge, MergeInsert:
				errChan <- scope.MergeRun(c)
			case Remote:
				errChan <- scope.RemoteRun(c)
			case Parallel:
				errChan <- scope.ParallelRun(c)
			default:
				errChan <- moerr.NewInternalError(c.ctx, "unexpected scope Magic %d", scope.Magic)
			}
		})
		if errSubmit != nil {
			errChan <- errSubmit
			wg.Done()
		}
	}

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	var errReceiveChan chan error
	if len(s.RemoteReceivRegInfos) > 0 {
		errReceiveChan = make(chan error, len(s.RemoteReceivRegInfos))
		s.notifyAndReceiveFromRemote(&wg, errReceiveChan)
	}
	defer wg.Wait()

	p := pipeline.NewMerge(s.Instructions, s.Reg)
	if _, err := p.MergeRun(s.Proc); err != nil {
		select {
		case <-s.Proc.Ctx.Done():
		default:
			p.Cleanup(s.Proc, true, err)
			return err
		}
	}
	p.Cleanup(s.Proc, false, nil)

	// receive and check error from pre-scopes and remote scopes.
	preScopeCount := len(s.PreScopes)
	remoteScopeCount := len(s.RemoteReceivRegInfos)
	if remoteScopeCount == 0 {
		for i := 0; i < len(s.PreScopes); i++ {
			if err := <-errChan; err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
			preScopeCount--

		case err := <-errReceiveChan:
			if err != nil {
				return err
			}
			remoteScopeCount--
		}

		if preScopeCount == 0 && remoteScopeCount == 0 {
			return nil
		}
	}
}

// RemoteRun send the scope to a remote node for execution.
func (s *Scope) RemoteRun(c *Compile) error {
	if !s.canRemote(c, true) || !cnclient.IsCNClientReady() {
		return s.ParallelRun(c)
	}

	runtime.ProcessLevelRuntime().Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	p := pipeline.New(0, nil, s.Instructions, s.Reg)
	err := s.remoteRun(c)
	select {
	case <-s.Proc.Ctx.Done():
		// this clean-up action shouldn't be called before context check.
		// because the clean-up action will cancel the context, and error will be suppressed.
		p.Cleanup(s.Proc, err != nil, err)
		return nil

	default:
		p.Cleanup(s.Proc, err != nil, err)
		return err
	}
}

// ParallelRun run a pipeline in parallel.
func (s *Scope) ParallelRun(c *Compile) (err error) {
	var parallelScope *Scope

	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			c.proc.Error(c.ctx, "panic in scope run",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}

		// if codes run here, it means some error happens during build the parallel scope.
		// we should do clean work for source-scope to avoid receiver hung.
		if parallelScope == nil {
			pipeline.NewMerge(s.Instructions, s.Reg).Cleanup(s.Proc, true, err)
		}
	}()

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)

	switch {
	// probability 1: it's a JOIN pipeline.
	case s.IsJoin:
		parallelScope, err = buildJoinParallelRun(s, c)

	// probability 2: it's a LOAD pipeline.
	case s.IsLoad:
		parallelScope, err = buildLoadParallelRun(s, c)

	// probability 3: it's a SCAN pipeline.
	case s.DataSource != nil:
		parallelScope, err = buildScanParallelRun(s, c)

	// others.
	default:
		parallelScope, err = s, nil
	}

	if err != nil {
		return err
	}

	if parallelScope.Magic == Normal {
		return parallelScope.Run(c)
	}
	return parallelScope.MergeRun(c)
}

// buildJoinParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline to run a join in parallel.
func buildJoinParallelRun(s *Scope, c *Compile) (*Scope, error) {
	if c.execType == plan2.ExecTypeTP {
		//tp query build scope in compile time, not runtime
		return s, nil
	}
	mcpu := s.NodeInfo.Mcpu
	if mcpu <= 1 { // no need to parallel
		buildScope := c.newJoinBuildScope(s, nil)
		s.PreScopes = append(s.PreScopes, buildScope)
		if s.BuildIdx > 1 {
			probeScope := c.newJoinProbeScope(s, nil)
			s.PreScopes = append(s.PreScopes, probeScope)
		}
		return s, nil
	}

	isRight := s.isRight()

	chp := s.PreScopes
	for i := range chp {
		chp[i].IsEnd = true
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Merge)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, 2, c.anal.Nodes())
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *process.RegisterMessage, 10)
	}
	probeScope, buildScope := c.newJoinProbeScope(s, ss), c.newJoinBuildScope(s, ss)

	ns, err := newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return nil, err
	}

	if isRight {
		channel := make(chan *bitmap.Bitmap, mcpu)
		for i := range ns.PreScopes {
			switch arg := ns.PreScopes[i].Instructions[0].Arg.(type) {
			case *right.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightsemi.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightanti.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}
			}
		}
	}
	ns.PreScopes = append(ns.PreScopes, chp...)
	ns.PreScopes = append(ns.PreScopes, buildScope)
	ns.PreScopes = append(ns.PreScopes, probeScope)

	return ns, nil
}

// buildLoadParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline to load in parallel.
func buildLoadParallelRun(s *Scope, c *Compile) (*Scope, error) {
	mcpu := s.NodeInfo.Mcpu
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Normal)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].DataSource = &Source{
			isConst: true,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
		if err := ss[i].initDataSource(c); err != nil {
			return nil, err
		}
	}

	ns, err := newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return nil, err
	}

	return ns, nil
}

// buildScanParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline which will get data from n scan-pipeline and output it as a while to the outside.
// return true if this was just one scan but not mergeScan.
func buildScanParallelRun(s *Scope, c *Compile) (*Scope, error) {
	// unexpected case.
	if s.IsRemote && len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.ctx, "ordered scan cannot run in remote.")
	}

	// receive runtime filter and optimized the datasource.
	if err := s.handleRuntimeFilter(c); err != nil {
		return nil, err
	}

	maxProvidedCpuNumber := goruntime.GOMAXPROCS(0)
	if c.execType == plan2.ExecTypeTP {
		maxProvidedCpuNumber = 1
	}

	var scanUsedCpuNumber int
	var readers []engine.Reader
	var err error

	switch {

	// If this was a remote-run pipeline. Reader should be generated from Engine.
	case s.IsRemote:
		// this cannot use c.ctx directly, please refer to `default case`.
		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		if s.DataSource.AccountId != nil {
			ctx = defines.AttachAccountId(ctx, uint32(s.DataSource.AccountId.GetTenantId()))
		}

		// determined how many cpus we should use.
		blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
		scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())

		readers, err = c.e.NewBlockReader(
			ctx, scanUsedCpuNumber,
			s.DataSource.Timestamp, s.DataSource.FilterExpr, s.NodeInfo.Data, s.DataSource.TableDef, c.proc)
		if err != nil {
			return nil, err
		}

	// Reader can be generated from local relation.
	case s.DataSource.Rel != nil && s.DataSource.TableDef.Partition == nil:
		switch s.DataSource.Rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, idSlice.Len())
		default:
			scanUsedCpuNumber = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			scanUsedCpuNumber = 1
		}

		readers, err = s.DataSource.Rel.NewReader(c.ctx,
			scanUsedCpuNumber,
			s.DataSource.FilterExpr,
			s.NodeInfo.Data,
			len(s.DataSource.OrderBy) > 0,
			s.TxnOffset)
		if err != nil {
			return nil, err
		}

	// Should get relation first to generate Reader.
	// FIXME:: s.NodeInfo.Rel == nil, partition table? -- this is an old comment, I just do a copy here.
	default:
		// This cannot modify the c.ctx here, but I don't know why.
		// Maybe there are some account related things stores in the context (using the context.WithValue),
		// and modify action will change the account.
		ctx := c.ctx

		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}

		var db engine.Database
		var rel engine.Relation
		// todo:
		//  these following codes were very likely to `compile.go:compileTableScanDataSource `.
		//  I kept the old codes here without any modify. I don't know if there is one `GetRelation(txn, scanNode, scheme, table)`
		{
			n := s.DataSource.node
			txnOp := s.Proc.TxnOperator
			if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
				if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
					n.ScanSnapshot.TS.Less(c.proc.TxnOperator.Txn().SnapshotTS) {

					txnOp = c.proc.TxnOperator.CloneSnapshotOp(*n.ScanSnapshot.TS)
					if n.ScanSnapshot.Tenant != nil {
						ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
					}
				}
			}

			db, err = c.e.Database(ctx, s.DataSource.SchemaName, txnOp)
			if err != nil {
				return nil, err
			}
			rel, err = db.Relation(ctx, s.DataSource.RelationName, c.proc)
			if err != nil {
				var e error // avoid contamination of error messages
				db, e = c.e.Database(ctx, defines.TEMPORARY_DBNAME, s.Proc.TxnOperator)
				if e != nil {
					return nil, e
				}
				rel, e = db.Relation(ctx, engine.GetTempTableName(s.DataSource.SchemaName, s.DataSource.RelationName), c.proc)
				if e != nil {
					return nil, err
				}
			}
		}

		switch rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, idSlice.Len())
		default:
			scanUsedCpuNumber = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			scanUsedCpuNumber = 1
		}

		if rel.GetEngineType() == engine.Memory || s.DataSource.PartitionRelationNames == nil {
			mainRds, err1 := rel.NewReader(ctx,
				scanUsedCpuNumber,
				s.DataSource.FilterExpr,
				s.NodeInfo.Data,
				len(s.DataSource.OrderBy) > 0,
				s.TxnOffset)
			if err1 != nil {
				return nil, err1
			}
			readers = append(readers, mainRds...)
		} else {
			// handle the partition table.
			blkArray := objectio.BlockInfoSlice(s.NodeInfo.Data)
			dirtyRanges := make(map[int]objectio.BlockInfoSlice)
			cleanRanges := make(objectio.BlockInfoSlice, 0, blkArray.Len())
			ranges := objectio.BlockInfoSlice(blkArray.Slice(1, blkArray.Len()))
			for i := 0; i < ranges.Len(); i++ {
				blkInfo := ranges.Get(i)
				if !blkInfo.CanRemote {
					if _, ok := dirtyRanges[blkInfo.PartitionNum]; !ok {
						newRanges := make(objectio.BlockInfoSlice, 0, objectio.BlockInfoSize)
						newRanges = append(newRanges, objectio.EmptyBlockInfoBytes...)
						dirtyRanges[blkInfo.PartitionNum] = newRanges
					}
					dirtyRanges[blkInfo.PartitionNum] = append(dirtyRanges[blkInfo.PartitionNum], ranges.GetBytes(i)...)
					continue
				}
				cleanRanges = append(cleanRanges, ranges.GetBytes(i)...)
			}

			if len(cleanRanges) > 0 {
				// create readers for reading clean blocks from the main table.
				mainRds, err1 := rel.NewReader(ctx,
					scanUsedCpuNumber,
					s.DataSource.FilterExpr,
					cleanRanges,
					len(s.DataSource.OrderBy) > 0,
					s.TxnOffset)
				if err1 != nil {
					return nil, err1
				}
				readers = append(readers, mainRds...)
			}
			// create readers for reading dirty blocks from partition table.
			for num, relName := range s.DataSource.PartitionRelationNames {
				subRel, err1 := db.Relation(ctx, relName, c.proc)
				if err1 != nil {
					return nil, err1
				}
				memRds, err2 := subRel.NewReader(ctx,
					scanUsedCpuNumber,
					s.DataSource.FilterExpr,
					dirtyRanges[num],
					len(s.DataSource.OrderBy) > 0,
					s.TxnOffset)
				if err2 != nil {
					return nil, err2
				}
				readers = append(readers, memRds...)
			}
		}
	}
	// just for quick GC.
	s.NodeInfo.Data = nil

	// need some merge to make sure it is only scanUsedCpuNumber reader.
	// partition table and read from memory will cause len(readers) > scanUsedCpuNumber.
	if len(readers) != scanUsedCpuNumber {
		newReaders := make([]engine.Reader, 0, scanUsedCpuNumber)
		step := len(readers) / scanUsedCpuNumber
		for i := 0; i < len(readers); i += step {
			newReaders = append(newReaders, disttae.NewMergeReader(readers[i:i+step]))
		}
		readers = newReaders
	}

	// only one scan reader, it can just run without any merge.
	if scanUsedCpuNumber == 1 {
		s.Magic = Normal
		s.DataSource.R = readers[0]
		s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
		return s, nil
	}

	if len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.ctx, "ordered scan must run in only one parallel.")
	}

	// return a pipeline which merge result from scanUsedCpuNumber scan.
	readerScopes := make([]*Scope, scanUsedCpuNumber)
	for i := 0; i < scanUsedCpuNumber; i++ {
		readerScopes[i] = newScope(Normal)
		readerScopes[i].NodeInfo = s.NodeInfo
		readerScopes[i].DataSource = &Source{
			R:            readers[i],
			SchemaName:   s.DataSource.SchemaName,
			RelationName: s.DataSource.RelationName,
			Attributes:   s.DataSource.Attributes,
			AccountId:    s.DataSource.AccountId,
		}
		readerScopes[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
		readerScopes[i].TxnOffset = s.TxnOffset
	}

	mergeFromParallelScanScope, errNew := newParallelScope(c, s, readerScopes)
	if errNew != nil {
		ReleaseScopes(readerScopes)
		return nil, err
	}
	mergeFromParallelScanScope.SetContextRecursively(s.Proc.Ctx)
	return mergeFromParallelScanScope, nil
}

func DetermineRuntimeDOP(cpunum, blocks int) int {
	if cpunum <= 0 || blocks <= 16 {
		return 1
	}
	ret := blocks/16 + 1
	if ret < cpunum {
		return ret
	}
	return cpunum
}

func (s *Scope) handleRuntimeFilter(c *Compile) error {
	var err error
	var inExprList []*plan.Expr
	exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterSpecs))
	filters := make([]process.RuntimeFilterMessage, 0, len(exprs))

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			msgReceiver := c.proc.NewMessageReceiver([]int32{spec.Tag}, process.AddrBroadCastOnCurrentCN())
			msgs, ctxDone := msgReceiver.ReceiveMessage(true, s.Proc.Ctx)
			if ctxDone {
				return nil
			}
			for i := range msgs {
				msg, ok := msgs[i].(process.RuntimeFilterMessage)
				if !ok {
					panic("expect runtime filter message, receive unknown message!")
				}
				switch msg.Typ {
				case process.RuntimeFilter_PASS:
					continue
				case process.RuntimeFilter_DROP:
					// FIXME: Should give an empty "Data" and then early return
					s.NodeInfo.Data = nil
					s.NodeInfo.NeedExpandRanges = false
					s.DataSource.FilterExpr = plan2.MakeFalseExpr()
					return nil
				case process.RuntimeFilter_IN:
					inExpr := plan2.MakeInExpr(c.ctx, spec.Expr, msg.Card, msg.Data, spec.MatchPrefix)
					inExprList = append(inExprList, inExpr)

					// TODO: implement BETWEEN expression
				}
				exprs = append(exprs, spec.Expr)
				filters = append(filters, msg)
			}
			msgReceiver.Free()
		}
	}

	for i := range inExprList {
		fn := inExprList[i].GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}

		newExpr := plan2.DeepCopyExpr(inExprList[i])
		//put expr in reader
		newExprList := []*plan.Expr{newExpr}
		if s.DataSource.FilterExpr != nil {
			newExprList = append(newExprList, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(newExprList)

		isFilterOnPK := s.DataSource.TableDef.Pkey != nil && col.Name == s.DataSource.TableDef.Pkey.PkeyColName
		if !isFilterOnPK {
			// put expr in filter instruction
			ins := s.Instructions[0]
			arg, ok := ins.Arg.(*filter.Argument)
			if !ok {
				panic("missing instruction for runtime filter!")
			}
			newExprList := []*plan.Expr{newExpr}
			if arg.E != nil {
				newExprList = append(newExprList, arg.E)
			}
			arg.E = colexec.RewriteFilterExprList(newExprList)
		}
	}

	if s.NodeInfo.NeedExpandRanges {
		scanNode := s.DataSource.node
		if scanNode == nil {
			panic("can not expand ranges on remote pipeline!")
		}

		newExprList := plan2.DeepCopyExprList(inExprList)
		if len(s.DataSource.node.BlockFilterList) > 0 {
			newExprList = append(newExprList, s.DataSource.node.BlockFilterList...)
		}

		ranges, err := c.expandRanges(s.DataSource.node, s.DataSource.Rel, newExprList)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = append(s.NodeInfo.Data, ranges.GetAllBytes()...)
		s.NodeInfo.NeedExpandRanges = false

	} else if len(inExprList) > 0 {
		s.NodeInfo.Data, err = ApplyRuntimeFilters(c.ctx, s.Proc, s.DataSource.TableDef, s.NodeInfo.Data, exprs, filters)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) isShuffle() bool {
	// the pipeline is merge->group->xxx
	if s != nil && len(s.Instructions) > 1 && (s.Instructions[1].Op == vm.Group) {
		arg := s.Instructions[1].Arg.(*group.Argument)
		return arg.IsShuffle
	}
	return false
}

func (s *Scope) isRight() bool {
	return s != nil && (s.Instructions[0].Op == vm.Right || s.Instructions[0].Op == vm.RightSemi || s.Instructions[0].Op == vm.RightAnti)
}

func newParallelScope(c *Compile, s *Scope, ss []*Scope) (*Scope, error) {
	var flg bool

	idx := 0
	defer func(ins vm.Instructions) {
		for i := 0; i < idx; i++ {
			if ins[i].Arg != nil {
				ins[i].Arg.Release()
			}
		}
	}(s.Instructions)

	for i, in := range s.Instructions {
		if flg {
			break
		}
		switch in.Op {
		case vm.Top:
			flg = true
			idx = i
			arg := in.Arg.(*top.Argument)
			// release the useless arg
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeTop,
				Idx: in.Idx,
				Arg: mergetop.NewArgument().
					WithFs(arg.Fs).
					WithLimit(arg.Limit),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				newarg := top.NewArgument().WithFs(arg.Fs).WithLimit(arg.Limit)
				newarg.TopValueTag = arg.TopValueTag
				ss[j].appendInstruction(vm.Instruction{
					Op:          vm.Top,
					Idx:         in.Idx,
					IsFirst:     in.IsFirst,
					Arg:         newarg,
					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		// case vm.Order:
		// there is no need to do special merge for order, because the behavior of order is just sort for each batch.
		case vm.Limit:
			flg = true
			idx = i
			arg := in.Arg.(*limit.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeLimit,
				Idx: in.Idx,
				Arg: mergelimit.NewArgument().
					WithLimit(arg.LimitExpr),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Limit,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: limit.NewArgument().
						WithLimit(arg.LimitExpr),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Group:
			flg = true
			idx = i
			arg := in.Arg.(*group.Argument)
			if arg.AnyDistinctAgg() {
				continue
			}
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeGroup,
				Idx: in.Idx,
				Arg: mergegroup.NewArgument().
					WithNeedEval(false),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Group,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: group.NewArgument().
						WithExprs(arg.Exprs).
						WithTypes(arg.Types).
						WithAggsNew(arg.Aggs),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Sample:
			arg := in.Arg.(*sample.Argument)
			if !arg.IsMergeSampleByRow() {
				flg = true
				idx = i
				// if by percent, there is no need to do merge sample.
				if arg.IsByPercent() {
					s.Instructions = s.Instructions[i:]
				} else {
					s.Instructions = append(make([]vm.Instruction, 1), s.Instructions[i:]...)
					s.Instructions[1] = vm.Instruction{
						Op:      vm.Sample,
						Idx:     in.Idx,
						IsFirst: false,
						Arg:     sample.NewMergeSample(arg, arg.NeedOutputRowSeen),

						CnAddr:      in.CnAddr,
						OperatorID:  c.allocOperatorID(),
						ParallelID:  0,
						MaxParallel: 1,
					}
				}
				s.Instructions[0] = vm.Instruction{
					Op:  vm.Merge,
					Idx: s.Instructions[0].Idx,
					Arg: merge.NewArgument(),

					CnAddr:      in.CnAddr,
					OperatorID:  c.allocOperatorID(),
					ParallelID:  0,
					MaxParallel: 1,
				}

				for j := range ss {
					ss[j].appendInstruction(vm.Instruction{
						Op:      vm.Sample,
						Idx:     in.Idx,
						IsFirst: in.IsFirst,
						Arg:     arg.SimpleDup(),

						CnAddr:      in.CnAddr,
						OperatorID:  in.OperatorID,
						MaxParallel: int32(len(ss)),
						ParallelID:  int32(j),
					})
				}
			}
			arg.Release()
		case vm.Offset:
			flg = true
			idx = i
			arg := in.Arg.(*offset.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOffset,
				Idx: in.Idx,
				Arg: mergeoffset.NewArgument().
					WithOffset(arg.OffsetExpr),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Offset,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: offset.NewArgument().
						WithOffset(arg.OffsetExpr),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Output:
		default:
			for j := range ss {
				ss[j].appendInstruction(dupInstruction(&in, nil, j))
			}
		}
	}
	if !flg {
		for i := range ss {
			if arg := ss[i].Instructions[len(ss[i].Instructions)-1].Arg; arg != nil {
				arg.Release()
			}
			ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
		}
		if arg := s.Instructions[0].Arg; arg != nil {
			arg.Release()
		}
		s.Instructions[0] = vm.Instruction{
			Op:  vm.Merge,
			Idx: s.Instructions[0].Idx, // TODO: remove it
			Arg: merge.NewArgument(),

			CnAddr:      s.Instructions[0].CnAddr,
			OperatorID:  c.allocOperatorID(),
			ParallelID:  0,
			MaxParallel: 1,
		}
		// Add log for cn panic which reported on issue 10656
		// If you find this log is printed, please report the repro details
		if len(s.Instructions) < 2 {
			c.proc.Error(c.proc.Ctx, "the length of s.Instructions is too short!"+DebugShowScopes([]*Scope{s}),
				zap.String("stack", string(debug.Stack())),
			)
			return nil, moerr.NewInternalErrorNoCtx("the length of s.Instructions is too short !")
		}
		if len(s.Instructions)-1 != 1 && s.Instructions[1].Arg != nil {
			s.Instructions[1].Arg.Release()
		}
		s.Instructions[1] = s.Instructions[len(s.Instructions)-1]
		for i := 2; i < len(s.Instructions)-1; i++ {
			if arg := s.Instructions[i].Arg; arg != nil {
				arg.Release()
			}
		}
		s.Instructions = s.Instructions[:2]
	}
	s.Magic = Merge
	s.PreScopes = ss
	cnt := 0
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		cnt++
	}
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, cnt)
	{
		for i := 0; i < cnt; i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: s.Proc.Ctx,
				Ch:  make(chan *process.RegisterMessage, 1),
			}
		}
	}
	j := 0
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op: vm.Connector,
				Arg: connector.NewArgument().
					WithReg(s.Proc.Reg.MergeReceivers[j]),

				CnAddr:      ss[i].Instructions[0].CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			})
			j++
		}
	}
	return s, nil
}

func (s *Scope) appendInstruction(in vm.Instruction) {
	if !s.IsEnd {
		s.Instructions = append(s.Instructions, in)
	}
}

func (s *Scope) notifyAndReceiveFromRemote(wg *sync.WaitGroup, errChan chan error) {
	// if context has done, it means the user or other part of the pipeline stops this query.
	closeWithError := func(err error, reg *process.WaitRegister) {
		if reg != nil {
			select {
			case <-s.Proc.Ctx.Done():
			case reg.Ch <- nil:
			}
		}

		select {
		case <-s.Proc.Ctx.Done():
			errChan <- nil
		default:
			errChan <- err
		}
		wg.Done()
	}

	// start N goroutines to send notifications to remote nodes.
	// to notify the remote dispatch executor where its remote receivers are.
	// dispatch operator will use this stream connection to send data back.
	//
	// function `cnMessageHandle` at file `scopeRemoteRun.go` will handle the notification.
	for i := range s.RemoteReceivRegInfos {
		wg.Add(1)

		op := &s.RemoteReceivRegInfos[i]
		fromAddr := op.FromAddr
		receiverIdx := op.Idx
		uuid := op.Uuid[:]

		errSubmit := ants.Submit(
			func() {
				streamSender, errStream := cnclient.GetStreamSender(fromAddr)
				if errStream != nil {
					s.Proc.Errorf(s.Proc.Ctx, "Failed to get stream sender txnID=%s, err=%v",
						s.Proc.TxnOperator.Txn().DebugString(), errStream)
					closeWithError(errStream, s.Proc.Reg.MergeReceivers[receiverIdx])
					return
				}
				defer streamSender.Close(true)

				message := cnclient.AcquireMessage()
				message.Id = streamSender.ID()
				message.Cmd = pbpipeline.Method_PrepareDoneNotifyMessage
				message.Sid = pbpipeline.Status_Last
				message.Uuid = uuid

				if errSend := streamSender.Send(s.Proc.Ctx, message); errSend != nil {
					closeWithError(errSend, s.Proc.Reg.MergeReceivers[receiverIdx])
					return
				}

				messagesReceive, errReceive := streamSender.Receive()
				if errReceive != nil {
					closeWithError(errReceive, s.Proc.Reg.MergeReceivers[receiverIdx])
					return
				}

				err := receiveMsgAndForward(s.Proc, messagesReceive, s.Proc.Reg.MergeReceivers[receiverIdx].Ch)
				closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx])
			},
		)

		if errSubmit != nil {
			errChan <- errSubmit
			wg.Done()
		}
	}
}

func receiveMsgAndForward(proc *process.Process, receiveCh chan morpc.Message, forwardCh chan *process.RegisterMessage) error {
	var val morpc.Message
	var dataBuffer []byte
	var ok bool
	var m *pbpipeline.Message

	for {
		select {
		case <-proc.Ctx.Done():
			return nil

		case val, ok = <-receiveCh:
			if val == nil || !ok {
				return moerr.NewStreamClosedNoCtx()
			}
		}

		m, ok = val.(*pbpipeline.Message)
		if !ok {
			panic("unexpected message type for cn-server")
		}

		// receive an end message from remote
		if err := pbpipeline.GetMessageErrorInfo(m); err != nil {
			return err
		}

		// end message
		if m.IsEndMessage() {
			return nil
		}

		// normal receive
		if dataBuffer == nil {
			dataBuffer = m.Data
		} else {
			dataBuffer = append(dataBuffer, m.Data...)
		}

		switch m.GetSid() {
		case pbpipeline.Status_WaitingNext:
			continue
		case pbpipeline.Status_Last:
			if m.Checksum != crc32.ChecksumIEEE(dataBuffer) {
				return moerr.NewInternalError(proc.Ctx, "Packages delivered by morpc is broken")
			}
			bat, err := decodeBatch(proc.Mp(), dataBuffer)
			if err != nil {
				return err
			}
			if forwardCh == nil {
				// used for delete
				proc.SetInputBatch(bat)
			} else {
				msg := &process.RegisterMessage{Batch: bat}
				select {
				case <-proc.Ctx.Done():
					bat.Clean(proc.Mp())
					return nil

				case forwardCh <- msg:
				}
			}
			dataBuffer = nil
		}
	}
}

func (s *Scope) replace(c *Compile) error {
	tblName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.Name
	deleteCond := s.Plan.GetQuery().Nodes[0].ReplaceCtx.DeleteCond

	delAffectedRows := uint64(0)
	if deleteCond != "" {
		result, err := c.runSqlWithResult(fmt.Sprintf("delete from %s where %s", tblName, deleteCond))
		if err != nil {
			return err
		}
		delAffectedRows = result.AffectedRows
	}
	result, err := c.runSqlWithResult("insert " + c.sql[7:])
	if err != nil {
		return err
	}
	c.addAffectedRows(result.AffectedRows + delAffectedRows)
	return nil
}

func (s Scope) TypeName() string {
	return "compile.Scope"
}
