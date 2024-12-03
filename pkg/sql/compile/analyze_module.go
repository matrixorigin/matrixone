// Copyright 2024 Matrix Origin
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
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type AnalyzeModule struct {
	// curNodeIdx is the current Node index when compilePlanScope
	curNodeIdx int
	// isFirst is the first opeator in pipeline for plan Node
	isFirst        bool
	qry            *plan.Query
	phyPlan        *models.PhyPlan
	remotePhyPlans []models.PhyPlan
	// Added read-write lock
	mu               sync.RWMutex
	retryTimes       int
	explainPhyBuffer *bytes.Buffer
}

// Reset When Compile reused, reset AnalyzeModule to prevent resource accumulation
func (anal *AnalyzeModule) Reset() {
	if anal != nil {
		anal.phyPlan = nil
		anal.remotePhyPlans = nil
		anal.explainPhyBuffer = nil
		anal.retryTimes = 0
		for _, node := range anal.qry.Nodes {
			if node.AnalyzeInfo == nil {
				node.AnalyzeInfo = new(plan.AnalyzeInfo)
			} else {
				node.AnalyzeInfo.Reset()
			}
		}
	}

}

func (anal *AnalyzeModule) AppendRemotePhyPlan(remotePhyPlan models.PhyPlan) {
	anal.mu.Lock()
	defer anal.mu.Unlock()
	anal.remotePhyPlans = append(anal.remotePhyPlans, remotePhyPlan)
}

func (anal *AnalyzeModule) GetPhyPlan() *models.PhyPlan {
	return anal.phyPlan
}

func (anal *AnalyzeModule) GetExplainPhyBuffer() *bytes.Buffer {
	return anal.explainPhyBuffer
}

func (anal *AnalyzeModule) TypeName() string {
	return "compile.analyzeModule"
}

func newAnalyzeModule() *AnalyzeModule {
	return reuse.Alloc[AnalyzeModule](nil)
}

func (anal *AnalyzeModule) release() {
	// there are 3 situations to release analyzeInfo
	// 1 is free analyzeInfo of Local CN when release analyze
	// 2 is free analyzeInfo of remote CN before transfer back
	// 3 is free analyzeInfo of remote CN when errors happen before transfer back
	// this is situation 1
	//for i := range a.analInfos {
	//	reuse.Free[process.AnalyzeInfo](a.analInfos[i], nil)
	//}
	reuse.Free[AnalyzeModule](anal, nil)
}

func (c *Compile) initAnalyzeModule(qry *plan.Query) {
	if len(qry.Nodes) == 0 {
		panic("empty logic plan")
	}

	c.anal = newAnalyzeModule()
	c.anal.qry = qry
	c.anal.curNodeIdx = int(qry.Steps[0])
	for _, node := range c.anal.qry.Nodes {
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = new(plan.AnalyzeInfo)
		} else {
			node.AnalyzeInfo.Reset()
		}
	}
}

func (c *Compile) GetAnalyzeModule() *AnalyzeModule {
	return c.anal
}

// setAnalyzeCurrent Update the specific scopes's instruction to true
// then update the current idx
func (c *Compile) setAnalyzeCurrent(updateScopes []*Scope, nextId int) {
	if updateScopes != nil {
		updateScopesLastFlag(updateScopes)
	}

	c.anal.curNodeIdx = nextId
	c.anal.isFirst = true
}

func updateScopesLastFlag(updateScopes []*Scope) {
	for _, s := range updateScopes {
		if s.RootOp == nil {
			continue
		}
		s.RootOp.GetOperatorBase().IsLast = true
	}
}

// applyOpStatsToNode Recursive traversal of PhyOperator tree,
// and add OpStats statistics to the corresponding NodeAnalyze Info
func applyOpStatsToNode(op *models.PhyOperator, nodes []*plan.Node, scopeParalleInfo *ParallelScopeInfo) {
	if op == nil {
		return
	}

	// Search for Plan Node based on NodeIdx and add OpStats statistical information to NodeAnalyze Info
	if op.NodeIdx >= 0 && op.NodeIdx < len(nodes) && op.OpStats != nil {
		node := nodes[op.NodeIdx]
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = &plan.AnalyzeInfo{}
		}
		node.AnalyzeInfo.InputRows += op.OpStats.InputRows
		node.AnalyzeInfo.OutputRows += op.OpStats.OutputRows
		node.AnalyzeInfo.InputSize += op.OpStats.InputSize
		node.AnalyzeInfo.OutputSize += op.OpStats.OutputSize
		node.AnalyzeInfo.TimeConsumed += op.OpStats.TimeConsumed
		node.AnalyzeInfo.MemorySize += op.OpStats.MemorySize
		node.AnalyzeInfo.WaitTimeConsumed += op.OpStats.WaitTimeConsumed
		node.AnalyzeInfo.ScanBytes += op.OpStats.ScanBytes
		node.AnalyzeInfo.NetworkIO += op.OpStats.NetworkIO
		node.AnalyzeInfo.InputBlocks += op.OpStats.InputBlocks

		node.AnalyzeInfo.S3List += op.OpStats.S3List
		node.AnalyzeInfo.S3Head += op.OpStats.S3Head
		node.AnalyzeInfo.S3Put += op.OpStats.S3Put
		node.AnalyzeInfo.S3Get += op.OpStats.S3Get
		node.AnalyzeInfo.S3Delete += op.OpStats.S3Delete
		node.AnalyzeInfo.S3DeleteMul += op.OpStats.S3DeleteMul
		node.AnalyzeInfo.DiskIO += op.OpStats.DiskIO

		node.AnalyzeInfo.CacheRead += op.OpStats.CacheRead
		node.AnalyzeInfo.CacheHit += op.OpStats.CacheHit
		node.AnalyzeInfo.CacheMemoryRead += op.OpStats.CacheMemoryRead
		node.AnalyzeInfo.CacheMemoryHit += op.OpStats.CacheMemoryHit
		node.AnalyzeInfo.CacheDiskRead += op.OpStats.CacheDiskRead
		node.AnalyzeInfo.CacheDiskHit += op.OpStats.CacheDiskHit
		node.AnalyzeInfo.CacheRemoteRead += op.OpStats.CacheRemoteRead
		node.AnalyzeInfo.CacheRemoteHit += op.OpStats.CacheRemoteHit

		node.AnalyzeInfo.WrittenRows += op.OpStats.WrittenRows
		node.AnalyzeInfo.DeletedRows += op.OpStats.DeletedRows

		node.AnalyzeInfo.ScanTime += op.OpStats.GetMetricByKey(process.OpScanTime)
		node.AnalyzeInfo.InsertTime += op.OpStats.GetMetricByKey(process.OpInsertTime)
		node.AnalyzeInfo.WaitLockTime += op.OpStats.GetMetricByKey(process.OpWaitLockTime)

		if _, isMinorOp := vm.MinorOpMap[op.OpName]; isMinorOp {
			isMinor := true
			if op.OpName == vm.OperatorToStrMap[vm.Filter] {
				if op.OpName != vm.OperatorToStrMap[vm.TableScan] && op.OpName != vm.OperatorToStrMap[vm.External] {
					isMinor = false // restrict operator is minor only for scan
				}
			}
			if isMinor {
				scopeParalleInfo.NodeIdxTimeConsumeMinor[op.NodeIdx] += op.OpStats.TimeConsumed
			}
		} else if _, isMajorOp := vm.MajorOpMap[op.OpName]; isMajorOp {
			scopeParalleInfo.NodeIdxTimeConsumeMajor[op.NodeIdx] += op.OpStats.TimeConsumed
		}
	}

	// Recursive processing of sub operators
	for _, childOp := range op.Children {
		applyOpStatsToNode(childOp, nodes, scopeParalleInfo)
	}
}

// processPhyScope Recursive traversal of PhyScope and processing of PhyOperators within it
func processPhyScope(scope *models.PhyScope, nodes []*plan.Node, stats *statistic.StatsInfo) {
	if scope == nil {
		return
	}

	stats.AddScopePrepareDuration(scope.PrepareTimeConsumed)
	// handle current Scope operator pipeline
	if scope.RootOperator != nil {
		scopeParallInfo := NewParallelScopeInfo()
		applyOpStatsToNode(scope.RootOperator, nodes, scopeParallInfo)

		for nodeIdx, timeConsumeMajor := range scopeParallInfo.NodeIdxTimeConsumeMajor {
			nodes[nodeIdx].AnalyzeInfo.TimeConsumedArrayMajor = append(nodes[nodeIdx].AnalyzeInfo.TimeConsumedArrayMajor, timeConsumeMajor)
		}

		for nodeIdx, timeConsumeMinor := range scopeParallInfo.NodeIdxTimeConsumeMinor {
			nodes[nodeIdx].AnalyzeInfo.TimeConsumedArrayMinor = append(nodes[nodeIdx].AnalyzeInfo.TimeConsumedArrayMinor, timeConsumeMinor)
		}
	}

	// handle preScopes recursively
	for _, preScope := range scope.PreScopes {
		processPhyScope(&preScope, nodes, stats)
	}
}

func (c *Compile) fillPlanNodeAnalyzeInfo(stats *statistic.StatsInfo) {
	if c.anal == nil {
		return
	}

	// handle local scopes
	for _, localScope := range c.anal.phyPlan.LocalScope {
		processPhyScope(&localScope, c.anal.qry.Nodes, stats)
	}

	// handle remote run scopes
	for _, remoteScope := range c.anal.phyPlan.RemoteScope {
		processPhyScope(&remoteScope, c.anal.qry.Nodes, stats)
	}
}

//----------------------------------------------------------------------------------------------------------------------

func ConvertScopeToPhyScope(scope *Scope, receiverMap map[*process.WaitRegister]int) models.PhyScope {
	phyScope := models.PhyScope{
		Magic:        scope.Magic.String(),
		Mcpu:         int8(scope.NodeInfo.Mcpu),
		DataSource:   ConvertSourceToPhySource(scope.DataSource),
		PreScopes:    []models.PhyScope{},
		RootOperator: ConvertOperatorToPhyOperator(scope.RootOp, receiverMap),
	}

	if scope.ScopeAnalyzer != nil {
		phyScope.PrepareTimeConsumed = scope.ScopeAnalyzer.TimeConsumed
	}

	if scope.Proc != nil {
		phyScope.Receiver = getScopeReceiver(scope, scope.Proc.Reg.MergeReceivers, receiverMap)
	}

	for _, preScope := range scope.PreScopes {
		phyScope.PreScopes = append(phyScope.PreScopes, ConvertScopeToPhyScope(preScope, receiverMap))
	}

	return phyScope
}

func getScopeReceiver(s *Scope, rs []*process.WaitRegister, rmp map[*process.WaitRegister]int) []models.PhyReceiver {
	receivers := make([]models.PhyReceiver, 0)
	for i := range rs {
		remote := ""
		for _, u := range s.RemoteReceivRegInfos {
			if u.Idx == i {
				remote = u.Uuid.String()
				break
			}
		}
		if id, ok := rmp[rs[i]]; ok {
			receivers = append(receivers, models.PhyReceiver{
				Idx:        id,
				RemoteUuid: remote,
			})
		} else {
			receivers = append(receivers, models.PhyReceiver{
				Idx:        -1, // "unknown"
				RemoteUuid: remote,
			})
		}
	}
	return receivers
}

// ConvertOperatorToPhyOperator Convert Operator to PhyOperator
func ConvertOperatorToPhyOperator(op vm.Operator, rmp map[*process.WaitRegister]int) *models.PhyOperator {
	if op == nil {
		return nil
	}

	phyOp := &models.PhyOperator{
		OpName:       op.OpType().String(),
		NodeIdx:      op.GetOperatorBase().Idx,
		DestReceiver: getDestReceiver(op, rmp),
	}

	if op.GetOperatorBase().IsFirst {
		phyOp.Status |= 1 << 0
	}
	if op.GetOperatorBase().IsLast {
		phyOp.Status |= 1 << 1
	}

	if op.GetOperatorBase().OpAnalyzer != nil {
		phyOp.OpStats = op.GetOperatorBase().OpAnalyzer.GetOpStats()
	}

	children := op.GetOperatorBase().Children
	phyChildren := make([]*models.PhyOperator, len(children))
	for i, child := range children {
		phyChildren[i] = ConvertOperatorToPhyOperator(child, rmp)
	}

	phyOp.Children = phyChildren
	return phyOp
}

// getDestReceiver returns the DestReceiver of the current Operator
func getDestReceiver(op vm.Operator, mp map[*process.WaitRegister]int) []models.PhyReceiver {
	receivers := make([]models.PhyReceiver, 0)
	id := op.OpType()
	_, ok := debugInstructionNames[id]
	if ok {
		if id == vm.Connector {
			arg := op.(*connector.Connector)
			if receiverId, okk := mp[arg.Reg]; okk {
				//receivers = append(receivers, receiverId)
				receivers = append(receivers, models.PhyReceiver{
					Idx:        receiverId,
					RemoteUuid: "",
				})
			}
		}
		if id == vm.Dispatch {
			arg := op.(*dispatch.Dispatch)
			for i := range arg.LocalRegs {
				if receiverId, okk := mp[arg.LocalRegs[i]]; okk {
					//receivers = append(receivers, receiverId)
					receivers = append(receivers, models.PhyReceiver{
						Idx:        receiverId,
						RemoteUuid: "",
					})
				} else {
					receivers = append(receivers, models.PhyReceiver{
						Idx:        -1,
						RemoteUuid: "",
					})
				}
			}

			if len(arg.RemoteRegs) != 0 {
				for _, reg := range arg.RemoteRegs {
					receivers = append(receivers, models.PhyReceiver{
						Idx:        -2, // reg.NodeAddr
						RemoteUuid: reg.Uuid.String(),
					})
				}
			}
		}
	} else {
		panic("unkonw operator type")
	}
	return receivers
}

func ConvertSourceToPhySource(source *Source) *models.PhySource {
	if source == nil {
		return nil
	}
	return &models.PhySource{
		SchemaName:   source.SchemaName,
		RelationName: source.RelationName,
		Attributes:   source.Attributes,
	}
}

func (c *Compile) GenPhyPlan(runC *Compile) {
	var generateReceiverMap func(*Scope, map[*process.WaitRegister]int)
	generateReceiverMap = func(s *Scope, mp map[*process.WaitRegister]int) {
		for i := range s.PreScopes {
			generateReceiverMap(s.PreScopes[i], mp)
		}
		if s.Proc == nil {
			return
		}
		for i := range s.Proc.Reg.MergeReceivers {
			mp[s.Proc.Reg.MergeReceivers[i]] = len(mp)
		}
	}

	receiverMap := make(map[*process.WaitRegister]int)
	ss := runC.scopes
	for i := range ss {
		generateReceiverMap(ss[i], receiverMap)
	}

	//------------------------------------------------------------------------------------------------------
	c.anal.phyPlan = models.NewPhyPlan()
	c.anal.phyPlan.RetryTime = runC.anal.retryTimes
	c.anal.curNodeIdx = runC.anal.curNodeIdx

	if len(runC.scopes) > 0 {
		for i := range runC.scopes {
			phyScope := ConvertScopeToPhyScope(runC.scopes[i], receiverMap)
			c.anal.phyPlan.LocalScope = append(c.anal.phyPlan.LocalScope, phyScope)
		}
	}

	// record the number of local cn s3 requests
	c.anal.phyPlan.S3IOInputCount += runC.counterSet.FileService.S3.Put.Load()
	c.anal.phyPlan.S3IOInputCount += runC.counterSet.FileService.S3.List.Load()

	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Head.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Get.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Delete.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.DeleteMulti.Load()
	//-------------------------------------------------------------------------------------------

	// record the number of remote cn s3 requests
	for _, remotePhy := range runC.anal.remotePhyPlans {
		c.anal.phyPlan.RemoteScope = append(c.anal.phyPlan.RemoteScope, remotePhy.LocalScope[0])
		c.anal.phyPlan.S3IOInputCount += remotePhy.S3IOInputCount
		c.anal.phyPlan.S3IOOutputCount += remotePhy.S3IOOutputCount
	}
}

type ParallelScopeInfo struct {
	NodeIdxTimeConsumeMajor map[int]int64
	NodeIdxTimeConsumeMinor map[int]int64
	//scopeId                 int32
}

func NewParallelScopeInfo() *ParallelScopeInfo {
	return &ParallelScopeInfo{
		NodeIdxTimeConsumeMajor: make(map[int]int64),
		NodeIdxTimeConsumeMinor: make(map[int]int64),
	}
}

//---------------------------------------------------------------------------------------------------

type ExplainOption struct {
	Verbose bool
	Analyze bool
}

func getExplainOption(options []tree.OptionElem) *ExplainOption {
	es := new(ExplainOption)

	if options == nil {
		return es
	}

	for _, v := range options {
		switch {
		case strings.EqualFold(v.Name, "VERBOSE"):
			es.Verbose = strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL"
		case strings.EqualFold(v.Name, "ANALYZE"):
			es.Analyze = strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL"
		}
	}

	return es
}

// makeExplainPhyPlanBuffer used to explain phyplan statement
func makeExplainPhyPlanBuffer(ss []*Scope, queryResult *util.RunResult, statsInfo *statistic.StatsInfo, anal *AnalyzeModule, option *ExplainOption) *bytes.Buffer {
	receiverMap := make(map[*process.WaitRegister]int)
	for i := range ss {
		genReceiverMap(ss[i], receiverMap)
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 300))

	//explainGlobalResources(queryResult, statsInfo, anal, option, buffer)
	explainResourceOverview(queryResult, statsInfo, anal, option, buffer)
	explainScopes(ss, 0, receiverMap, option, buffer)
	return buffer
}

func explainResourceOverview(queryResult *util.RunResult, statsInfo *statistic.StatsInfo, anal *AnalyzeModule, option *ExplainOption, buffer *bytes.Buffer) {
	if option.Analyze || option.Verbose {
		gblStats := models.ExtractPhyPlanGlbStats(anal.phyPlan)
		buffer.WriteString("Overview:\n")
		buffer.WriteString(fmt.Sprintf("\tMemoryUsage:%dB,  DiskI/O:%dB,  NewWorkI/O:%dB, AffectedRows: %d",
			gblStats.MemorySize,
			gblStats.DiskIOSize,
			gblStats.NetWorkSize,
			queryResult.AffectRows,
		))

		if statsInfo != nil {
			buffer.WriteString("\n")
			// Calculate the total sum of S3 requests for each stage
			list, head, put, get, delete, deleteMul, writtenRows, deletedRows := models.CalcTotalS3Requests(gblStats, statsInfo)

			s3InputEstByRows := objectio.EstimateS3Input(writtenRows)
			buffer.WriteString(fmt.Sprintf("\tS3List:%d, S3Head:%d, S3Put:%d, S3Get:%d, S3Delete:%d, S3DeleteMul:%d, S3InputEstByRows((%d+%d)/8192):%.4f \n",
				list, head, put, get, delete, deleteMul, writtenRows, deletedRows, s3InputEstByRows,
			))

			cpuTimeVal := gblStats.OperatorTimeConsumed +
				int64(statsInfo.ParseStage.ParseDuration+statsInfo.PlanStage.PlanDuration+statsInfo.CompileStage.CompileDuration) +
				statsInfo.PrepareRunStage.ScopePrepareDuration + statsInfo.PrepareRunStage.CompilePreRunOnceDuration -
				statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock -
				(statsInfo.IOAccessTimeConsumption + statsInfo.S3FSPrefetchFileIOMergerTimeConsumption)

			buffer.WriteString("\tCPU Usage: \n")
			buffer.WriteString(fmt.Sprintf("\t\t- Total CPU Time: %dns \n", cpuTimeVal))
			buffer.WriteString(fmt.Sprintf("\t\t- CPU Time Detail: Parse(%d)+BuildPlan(%d)+Compile(%d)+PhyExec(%d)+PrepareRun(%d)-PreRunWaitLock(%d)-IOAccess(%d)-IOMerge(%d)\n",
				statsInfo.ParseStage.ParseDuration,
				statsInfo.PlanStage.PlanDuration,
				statsInfo.CompileStage.CompileDuration,
				gblStats.OperatorTimeConsumed,
				gblStats.ScopePrepareTimeConsumed+statsInfo.PrepareRunStage.CompilePreRunOnceDuration,
				statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock,
				statsInfo.IOAccessTimeConsumption,
				statsInfo.S3FSPrefetchFileIOMergerTimeConsumption))

			//-------------------------------------------------------------------------------------------------------
			if option.Analyze {
				buffer.WriteString("\tQuery Build Plan Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %dns \n", statsInfo.PlanStage.PlanDuration))
				buffer.WriteString(fmt.Sprintf("\t\t- S3List:%d, S3Head:%d, S3Put:%d, S3Get:%d, S3Delete:%d, S3DeleteMul:%d\n",
					statsInfo.PlanStage.BuildPlanS3Request.List,
					statsInfo.PlanStage.BuildPlanS3Request.Head,
					statsInfo.PlanStage.BuildPlanS3Request.Put,
					statsInfo.PlanStage.BuildPlanS3Request.Get,
					statsInfo.PlanStage.BuildPlanS3Request.Delete,
					statsInfo.PlanStage.BuildPlanS3Request.DeleteMul,
				))
				buffer.WriteString(fmt.Sprintf("\t\t- Call Stats Duration: %dns \n", statsInfo.PlanStage.BuildPlanStatsDuration))

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Compile Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %dns \n", statsInfo.CompileStage.CompileDuration))
				buffer.WriteString(fmt.Sprintf("\t\t- S3List:%d, S3Head:%d, S3Put:%d, S3Get:%d, S3Delete:%d, S3DeleteMul:%d\n",
					statsInfo.CompileStage.CompileS3Request.List,
					statsInfo.CompileStage.CompileS3Request.Head,
					statsInfo.CompileStage.CompileS3Request.Put,
					statsInfo.CompileStage.CompileS3Request.Get,
					statsInfo.CompileStage.CompileS3Request.Delete,
					statsInfo.CompileStage.CompileS3Request.DeleteMul,
				))
				buffer.WriteString(fmt.Sprintf("\t\t- Compile TableScan Duration: %dns \n", statsInfo.CompileStage.CompileTableScanDuration))

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Prepare Exec Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %dns \n", gblStats.ScopePrepareTimeConsumed+statsInfo.PrepareRunStage.CompilePreRunOnceDuration-statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock))
				buffer.WriteString(fmt.Sprintf("\t\t- CompilePreRunOnce Duration: %dns \n", statsInfo.PrepareRunStage.CompilePreRunOnceDuration))
				buffer.WriteString(fmt.Sprintf("\t\t- PreRunOnce WaitLock: %dns \n", statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock))
				buffer.WriteString(fmt.Sprintf("\t\t- ScopePrepareTimeConsumed: %dns \n", gblStats.ScopePrepareTimeConsumed))
				buffer.WriteString(fmt.Sprintf("\t\t- BuildReader Duration: %dns \n", statsInfo.PrepareRunStage.BuildReaderDuration))
				buffer.WriteString(fmt.Sprintf("\t\t- S3List:%d, S3Head:%d, S3Put:%d, S3Get:%d, S3Delete:%d, S3DeleteMul:%d\n",
					statsInfo.PrepareRunStage.ScopePrepareS3Request.List,
					statsInfo.PrepareRunStage.ScopePrepareS3Request.Head,
					statsInfo.PrepareRunStage.ScopePrepareS3Request.Put,
					statsInfo.PrepareRunStage.ScopePrepareS3Request.Get,
					statsInfo.PrepareRunStage.ScopePrepareS3Request.Delete,
					statsInfo.PrepareRunStage.ScopePrepareS3Request.DeleteMul,
				))

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Execution Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %dns \n", gblStats.OperatorTimeConsumed))
				buffer.WriteString(fmt.Sprintf("\t\t- S3List:%d, S3Head:%d, S3Put:%d, S3Get:%d, S3Delete:%d, S3DeleteMul:%d\n",
					gblStats.S3ListRequest,
					gblStats.S3HeadRequest,
					gblStats.S3PutRequest,
					gblStats.S3GetRequest,
					gblStats.S3DeleteRequest,
					gblStats.S3DeleteMultiRequest,
				))

				buffer.WriteString(fmt.Sprintf("\t\t- MemoryUsage: %dB,  DiskI/O: %dB,  NewWorkI/O:%dB\n",
					gblStats.MemorySize,
					gblStats.DiskIOSize,
					gblStats.NetWorkSize,
				))
			}
			//-------------------------------------------------------------------------------------------------------
			buffer.WriteString("Physical Plan Deployment:")
		}
	}
}

func explainScopes(scopes []*Scope, gap int, rmp map[*process.WaitRegister]int, option *ExplainOption, buffer *bytes.Buffer) {
	for i := range scopes {
		explainSingleScope(scopes[i], i, gap, rmp, option, buffer)
	}
}

// explainSingleScope generates and outputs a string representation of a single Scope.
// It includes header information of Scope, data source information, and pipeline tree information.
// In addition, it recursively displays information from any PreScopes.
func explainSingleScope(scope *Scope, index int, gap int, rmp map[*process.WaitRegister]int, option *ExplainOption, buffer *bytes.Buffer) {
	gapNextLine(gap, buffer)

	// Scope Header
	receiverStr := "nil"
	if scope.Proc != nil {
		receiverStr = getReceiverStr(scope, scope.Proc.Reg.MergeReceivers, rmp)
	}

	if option.Verbose || option.Analyze {
		buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, addr:%v, mcpu: %v, Receiver: %s)", index+1, magicShow(scope.Magic), scope.NodeInfo.Addr, scope.NodeInfo.Mcpu, receiverStr))
		if scope.ScopeAnalyzer != nil {
			buffer.WriteString(fmt.Sprintf(" PrepareTimeConsumed: %dns", scope.ScopeAnalyzer.TimeConsumed))
		} else {
			buffer.WriteString(" PrepareTimeConsumed: 0ns")
		}
	} else {
		buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v, Receiver: %s)", index+1, magicShow(scope.Magic), scope.NodeInfo.Mcpu, receiverStr))
	}

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOp != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		explainPipeline(scope.RootOp, prefixStr, true, true, rmp, option, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString("  PreScopes: {")
		for i := range scope.PreScopes {
			explainSingleScope(scope.PreScopes[i], i, gap+4, rmp, option, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString("  }")
	}
}

func explainPipeline(node vm.Operator, prefix string, isRoot bool, isTail bool, mp map[*process.WaitRegister]int, option *ExplainOption, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	id := node.OpType()
	name, ok := debugInstructionNames[id]
	if !ok {
		name = "unknown"
	}

	analyzeStr := ""
	if option.Verbose {
		analyzeStr = fmt.Sprintf("(idx:%v, isFirst:%v, isLast:%v)",
			node.GetOperatorBase().Idx,
			node.GetOperatorBase().IsFirst,
			node.GetOperatorBase().IsLast)
	}
	if option.Analyze {
		if node.GetOperatorBase().OpAnalyzer != nil && node.GetOperatorBase().OpAnalyzer.GetOpStats() != nil {
			analyzeStr += node.GetOperatorBase().OpAnalyzer.GetOpStats().String()
		}
	}

	// Write to the current node
	if isRoot {
		headPrefix := "  Pipeline: └── "
		buffer.WriteString(fmt.Sprintf("%s%s%s", headPrefix, name, analyzeStr))
		hanldeTailNodeReceiver(node, mp, buffer)
		buffer.WriteString("\n")
		// Ensure that child nodes are properly indented
		prefix += "   "
	} else {
		if isTail {
			buffer.WriteString(fmt.Sprintf("%s└── %s%s", prefix, name, analyzeStr))
			hanldeTailNodeReceiver(node, mp, buffer)
			buffer.WriteString("\n")
		} else {
			buffer.WriteString(fmt.Sprintf("%s├── %s%s\n", prefix, name, analyzeStr))
		}
	}

	// Calculate new prefix
	newPrefix := prefix
	if isTail {
		newPrefix += "    "
	} else {
		newPrefix += "│   "
	}

	// Write to child node
	for i := 0; i < len(node.GetOperatorBase().Children); i++ {
		isLast := i == len(node.GetOperatorBase().Children)-1
		explainPipeline(node.GetOperatorBase().GetChildren(i), newPrefix, false, isLast, mp, option, buffer)
	}

	if isRoot {
		trimLastNewline(buffer)
	}
}
