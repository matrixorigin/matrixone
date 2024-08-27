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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type AnalyzeModuleV1 struct {
	// curNodeIdx is the current Node index when compilePlanScope
	curNodeIdx int
	// isFirst is the first opeator in pipeline for plan Node
	isFirst        bool
	qry            *plan.Query
	phyPlan        *models.PhyPlan
	remotePhyPlans []models.PhyPlan
	// Added read-write lock
	mu sync.RWMutex
}

func (anal *AnalyzeModuleV1) AppendRemotePhyPlan(remotePhyPlan models.PhyPlan) {
	anal.mu.Lock()
	defer anal.mu.Unlock()
	anal.remotePhyPlans = append(anal.remotePhyPlans, remotePhyPlan)
}

func (anal *AnalyzeModuleV1) GetPhyPlan() *models.PhyPlan {
	return anal.phyPlan
}

func (a AnalyzeModuleV1) TypeName() string {
	return "compile.analyzeModuleV1"
}

func newAnalyzeModuleV1() *AnalyzeModuleV1 {
	return reuse.Alloc[AnalyzeModuleV1](nil)
}

func (a *AnalyzeModuleV1) release() {
	// there are 3 situations to release analyzeInfo
	// 1 is free analyzeInfo of Local CN when release analyze
	// 2 is free analyzeInfo of remote CN before transfer back
	// 3 is free analyzeInfo of remote CN when errors happen before transfer back
	// this is situation 1
	//for i := range a.analInfos {
	//	reuse.Free[process.AnalyzeInfo](a.analInfos[i], nil)
	//}
	reuse.Free[AnalyzeModuleV1](a, nil)
}

func (c *Compile) initAnalyzeModuleV1(qry *plan.Query) {
	if len(qry.Nodes) == 0 {
		panic("empty logic plan")
	}

	c.anal = newAnalyzeModuleV1()
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

func (c *Compile) GetAnalyzeModuleV1() *AnalyzeModuleV1 {
	return c.anal
}

func (c *Compile) GetOriginSql() string {
	return c.sql
}

func (c *Compile) setAnalyzeCurrentV1(updateScopes []*Scope, nextId int) {
	if updateScopes != nil {
		updateScopesLastFlag(updateScopes)
	}

	c.anal.curNodeIdx = nextId
	c.anal.isFirst = true
}

// addOpStatsToPlanNodes Recursive traversal of PhyOperator and adding OpStats statistics to the corresponding NodeAnalyze Info
func addOpStatsToPlanNodes(op *models.PhyOperator, nodes []*plan.Node) {
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
		node.AnalyzeInfo.DiskIO += op.OpStats.TotalDiskIO
		node.AnalyzeInfo.S3IOByte += op.OpStats.TotalS3IOByte
		node.AnalyzeInfo.S3IOInputCount += op.OpStats.TotalS3InputCount
		node.AnalyzeInfo.S3IOOutputCount += op.OpStats.TotalS3OutputCount
		node.AnalyzeInfo.NetworkIO += op.OpStats.TotalNetworkIO
		node.AnalyzeInfo.ScanTime += op.OpStats.TotalScanTime
		node.AnalyzeInfo.InsertTime += op.OpStats.TotalInsertTime
		node.AnalyzeInfo.InputBlocks += op.OpStats.TotalInputBlocks
	}

	// Recursive processing of sub operators
	for _, childOp := range op.Children {
		addOpStatsToPlanNodes(childOp, nodes)
	}
}

// processPhyScope Recursive traversal of PhyScope and processing of PhyOperators within it
func processPhyScope(scope *models.PhyScope, nodes []*plan.Node) {
	if scope == nil {
		return
	}

	// handle current Scope operator pipeline
	if scope.RootOperator != nil {
		addOpStatsToPlanNodes(scope.RootOperator, nodes)
	}

	// handle preScopes recursively
	for _, preScope := range scope.PreScopes {
		processPhyScope(&preScope, nodes)
	}
}

// Check if SQL has a query plan
func (c *Compile) checkSQLHasQueryPlan() bool {
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
				remote = fmt.Sprintf("%s", u.Uuid)
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

	//--------------------debug---------------------
	//if op.GetOperatorBase().OpAnalyzer == nil {
	//	buf := bytes.NewBuffer(make([]byte, 0, 50))
	//	op.String(buf)
	//	panic(fmt.Sprintf("<>operator analyzer is null opName: %v", buf.String()))
	//}
	//--------------------debug---------------------

	phyOp := &models.PhyOperator{
		OpName:       op.OpType().String(),
		NodeIdx:      op.GetOperatorBase().Idx,
		DestReceiver: getDestReceiver(op, rmp),
		//OpStats:      op.GetOperatorBase().OpAnalyzer.GetOpStats(),
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
					//receivers = append(receivers, -1)
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
						RemoteUuid: fmt.Sprintf("%s", reg.Uuid),
					})
					//fmt.Sprintf("[addr: %s, uuid %s]", reg.NodeAddr, reg.Uuid)
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

func (c *Compile) GeneratePhyPlan() {
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
	ss := c.scope
	for i := range ss {
		generateReceiverMap(ss[i], receiverMap)
	}
	//------------------------------------------------------------------------------------------------------
	c.anal.phyPlan = models.NewPhyPlan()
	if len(c.scope) > 0 {
		for i := range c.scope {
			phyScope := ConvertScopeToPhyScope(c.scope[i], receiverMap)
			c.anal.phyPlan.LocalScope = append(c.anal.phyPlan.LocalScope, phyScope)
		}
	}

	//-------------------------------------------------------------------------------------------
	// record the number of s3 requests
	c.anal.phyPlan.S3IOInputCount += c.counterSet.FileService.S3.Put.Load()
	c.anal.phyPlan.S3IOInputCount += c.counterSet.FileService.S3.List.Load()

	c.anal.phyPlan.S3IOOutputCount += c.counterSet.FileService.S3.Head.Load()
	c.anal.phyPlan.S3IOOutputCount += c.counterSet.FileService.S3.Get.Load()
	c.anal.phyPlan.S3IOOutputCount += c.counterSet.FileService.S3.Delete.Load()
	c.anal.phyPlan.S3IOOutputCount += c.counterSet.FileService.S3.DeleteMulti.Load()
	//-------------------------------------------------------------------------------------------

	for _, remotePhy := range c.anal.remotePhyPlans {
		c.anal.phyPlan.RemoteScope = append(c.anal.phyPlan.RemoteScope, remotePhy.LocalScope[0])
		c.anal.phyPlan.S3IOInputCount += remotePhy.S3IOInputCount
		c.anal.phyPlan.S3IOOutputCount += remotePhy.S3IOOutputCount
	}
}
