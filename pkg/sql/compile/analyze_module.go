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
		node.AnalyzeInfo.InputRows += op.OpStats.TotalInputRows
		node.AnalyzeInfo.OutputRows += op.OpStats.TotalOutputRows
		node.AnalyzeInfo.InputSize += op.OpStats.TotalInputSize
		node.AnalyzeInfo.OutputSize += op.OpStats.TotalOutputSize
		node.AnalyzeInfo.TimeConsumed += op.OpStats.TotalTimeConsumed
		node.AnalyzeInfo.MemorySize += op.OpStats.TotalMemorySize
		node.AnalyzeInfo.WaitTimeConsumed += op.OpStats.TotalWaitTimeConsumed
		node.AnalyzeInfo.ScanBytes += op.OpStats.TotalScanBytes
		node.AnalyzeInfo.NetworkIO += op.OpStats.TotalNetworkIO
		node.AnalyzeInfo.InputBlocks += op.OpStats.TotalInputBlocks
		node.AnalyzeInfo.ScanTime += op.OpStats.GetMetricByKey(process.OpScanTime)
		node.AnalyzeInfo.InsertTime += op.OpStats.GetMetricByKey(process.OpInsertTime)

		if _, isMinorOp := vm.MinorOpMap[op.OpName]; isMinorOp {
			isMinor := true
			if op.OpName == vm.OperatorToStrMap[vm.Filter] {
				if op.OpName != vm.OperatorToStrMap[vm.TableScan] && op.OpName != vm.OperatorToStrMap[vm.External] {
					isMinor = false // restrict operator is minor only for scan
				}
			}
			if isMinor {
				scopeParalleInfo.NodeIdxTimeConsumeMinor[op.NodeIdx] += op.OpStats.TotalTimeConsumed
			}
		} else if _, isMajorOp := vm.MajorOpMap[op.OpName]; isMajorOp {
			scopeParalleInfo.NodeIdxTimeConsumeMajor[op.NodeIdx] += op.OpStats.TotalTimeConsumed
		}
	}

	// Recursive processing of sub operators
	for _, childOp := range op.Children {
		applyOpStatsToNode(childOp, nodes, scopeParalleInfo)
	}
}

// processPhyScope Recursive traversal of PhyScope and processing of PhyOperators within it
func processPhyScope(scope *models.PhyScope, nodes []*plan.Node) {
	if scope == nil {
		return
	}

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
		processPhyScope(&preScope, nodes)
	}
}

// hasValidQueryPlan Check if SQL has a query plan
func (c *Compile) hasValidQueryPlan() bool {
	if qry, ok := c.pn.Plan.(*plan.Plan_Query); ok {
		if qry.Query.StmtType != plan.Query_REPLACE {
			return true
		}
	}
	return false
}

func (c *Compile) fillPlanNodeAnalyzeInfo() {
	if c.anal == nil {
		return
	}

	// handle local scopes
	for _, localScope := range c.anal.phyPlan.LocalScope {
		processPhyScope(&localScope, c.anal.qry.Nodes)
	}

	// handle remote run scopes
	for _, remoteScope := range c.anal.phyPlan.RemoteScope {
		processPhyScope(&remoteScope, c.anal.qry.Nodes)
	}

	// Summarize the S3 resources executed by SQL into curNode
	// TODO: Actually, S3 resources may not necessarily be used by the current node.
	// We will handle it this way for now and optimize it in the future
	curNode := c.anal.qry.Nodes[c.anal.curNodeIdx]
	curNode.AnalyzeInfo.S3IOInputCount = c.anal.phyPlan.S3IOInputCount
	curNode.AnalyzeInfo.S3IOOutputCount = c.anal.phyPlan.S3IOOutputCount
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

	// record the number of s3 requests
	c.anal.phyPlan.S3IOInputCount += runC.counterSet.FileService.S3.Put.Load()
	c.anal.phyPlan.S3IOInputCount += runC.counterSet.FileService.S3.List.Load()

	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Head.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Get.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.Delete.Load()
	c.anal.phyPlan.S3IOOutputCount += runC.counterSet.FileService.S3.DeleteMulti.Load()
	//-------------------------------------------------------------------------------------------

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

func makeExplainPhyPlanBuffer(ss []*Scope, queryResult *util.RunResult, statsInfo *statistic.StatsInfo, anal *AnalyzeModule, option *ExplainOption) *bytes.Buffer {
	receiverMap := make(map[*process.WaitRegister]int)
	for i := range ss {
		genReceiverMap(ss[i], receiverMap)
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 300))

	explainGlobalResources(queryResult, statsInfo, anal, option, buffer)
	explainScopes(ss, 0, receiverMap, option, buffer)
	return buffer
}

func explainGlobalResources(queryResult *util.RunResult, statsInfo *statistic.StatsInfo, anal *AnalyzeModule, option *ExplainOption, buffer *bytes.Buffer) {
	if option.Analyze {
		gblStats := extractPhyPlanGlbStats(anal.phyPlan)
		buffer.WriteString(fmt.Sprintf("PhyPlan TimeConsumed:%dns, MemorySize:%dbytes, S3InputCount: %d, S3OutputCount: %d, AffectRows: %d",
			gblStats.TimeConsumed,
			gblStats.MemorySize,
			anal.phyPlan.S3IOInputCount,
			anal.phyPlan.S3IOOutputCount,
			queryResult.AffectRows,
		))

		if statsInfo != nil {
			buffer.WriteString("\n")
			cpuTimeVal := gblStats.TimeConsumed + statsInfo.BuildReaderDuration +
				int64(statsInfo.ParseDuration+
					statsInfo.CompileDuration+
					statsInfo.PlanDuration) - (statsInfo.IOAccessTimeConsumption + statsInfo.IOMergerTimeConsumption())

			buffer.WriteString(fmt.Sprintf("StatsInfo：CpuTime(%dns) = PhyTime(%d)+BuildReaderTime(%d)+ParseTime(%d)+CompileTime(%d)+PlanTime(%d)-IOAccessTime(%d)-IOMergeTime(%d)\n",
				cpuTimeVal,
				gblStats.TimeConsumed,
				statsInfo.BuildReaderDuration,
				statsInfo.ParseDuration,
				statsInfo.CompileDuration,
				statsInfo.PlanDuration,
				statsInfo.IOAccessTimeConsumption,
				statsInfo.IOMergerTimeConsumption()))

			buffer.WriteString(fmt.Sprintf("PlanStatsDuration: %dns, PlanResolveVariableDuration: %dns",
				statsInfo.BuildPlanStatsDuration,
				statsInfo.BuildPlanResolveVarDuration,
			))
		}
	}
}

func explainScopes(scopes []*Scope, gap int, rmp map[*process.WaitRegister]int, option *ExplainOption, buffer *bytes.Buffer) {
	for i := range scopes {
		explainSingleScope(scopes[i], i, gap, rmp, option, buffer)
	}
}

// showSingleScope generates and outputs a string representation of a single Scope.
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

//----------------------------------------------------------------------------------------------------------------------

// Define a struct to hold the total time and wait time
type GblStats struct {
	TimeConsumed int64
	MemorySize   int64
}

// Function to recursively process PhyScope and extract stats from PhyOperator
func handlePhyOperator(op *models.PhyOperator, stats *GblStats) {
	if op == nil {
		return
	}

	// Accumulate stats from the current operator
	if op.OpStats != nil && op.NodeIdx >= 0 {
		stats.TimeConsumed += op.OpStats.TotalTimeConsumed
		stats.MemorySize += op.OpStats.TotalWaitTimeConsumed
	}

	// Recursively process child operators
	for _, childOp := range op.Children {
		handlePhyOperator(childOp, stats)
	}
}

// Function to process PhyScope (including PreScopes and RootOperator)
func handlePhyScope(scope *models.PhyScope, stats *GblStats) {
	// Process the RootOperator of the current scope
	handlePhyOperator(scope.RootOperator, stats)

	// Recursively process PreScopes (which are also PhyScopes)
	for _, preScope := range scope.PreScopes {
		handlePhyScope(&preScope, stats)
	}
}

// Function to process all scopes in a PhyPlan (LocalScope and RemoteScope)
func handlePhyPlanScopes(scopes []models.PhyScope, stats *GblStats) {
	for _, scope := range scopes {
		handlePhyScope(&scope, stats)
	}
}

// Function to process the entire PhyPlan and extract the stats
func extractPhyPlanGlbStats(plan *models.PhyPlan) GblStats {
	var stats GblStats

	// Process LocalScope
	handlePhyPlanScopes(plan.LocalScope, &stats)

	// Process RemoteScope
	handlePhyPlanScopes(plan.RemoteScope, &stats)

	return stats
}
