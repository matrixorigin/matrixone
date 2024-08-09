package compile

import (
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type AnalyzeModuleV1 struct {
	// curNodeIdx is the current Node index when compilePlanScope
	curNodeIdx int
	// isFirst is the first opeator in pipeline for plan Node
	isFirst bool
	qry     *plan.Query
	phyPlan PhyPlan
}

func (c *Compile) initAnalyzeModuleV1(qry *plan.Query) {
	if len(qry.Nodes) == 0 {
		panic("empty logic plan")
	}

	c.analV1 = &AnalyzeModuleV1{}
	c.analV1.qry = qry
	c.analV1.curNodeIdx = int(qry.Steps[0])
	for _, node := range c.analV1.qry.Nodes {
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = new(plan.AnalyzeInfo)
		}
	}
}

func (c *Compile) setAnalyzeCurrentV1(updateScopes []*Scope, nextId int) {
	if updateScopes != nil {
		updateScopesLastFlag(updateScopes)
	}

	c.analV1.curNodeIdx = nextId
	c.analV1.isFirst = true
}

// 递归遍历 PhyOperator 并将 OpStats 添加到对应的 Plan Node 中
func addOpStatsToPlanNodes(op *PhyOperator, nodes []*plan.Node) {
	if op == nil {
		return
	}

	// 根据 NodeIdx 查找对应的 Plan Node 并添加 OpStats
	if op.NodeIdx >= 0 && op.NodeIdx < len(nodes) && op.OpStats != nil {
		node := nodes[op.NodeIdx]
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = &plan.AnalyzeInfo{}
		}
		node.AnalyzeInfo.InputRows = op.OpStats.TotalInputRows
		node.AnalyzeInfo.OutputRows = op.OpStats.TotalOutputRows
		node.AnalyzeInfo.InputSize = op.OpStats.TotalInputSize
		node.AnalyzeInfo.OutputSize = op.OpStats.TotalOutputSize
		node.AnalyzeInfo.TimeConsumed = op.OpStats.TotalTimeConsumed
		node.AnalyzeInfo.MemorySize = op.OpStats.TotalMemorySize
		node.AnalyzeInfo.WaitTimeConsumed = op.OpStats.TotalWaitTimeConsumed
		node.AnalyzeInfo.DiskIO = op.OpStats.TotalDiskIO
		node.AnalyzeInfo.S3IOByte = op.OpStats.TotalS3IOByte
		node.AnalyzeInfo.S3IOInputCount = op.OpStats.TotalS3InputCount
		node.AnalyzeInfo.S3IOOutputCount = op.OpStats.TotalS3OutputCount
		node.AnalyzeInfo.NetworkIO = op.OpStats.TotalNetworkIO
		node.AnalyzeInfo.ScanTime = op.OpStats.TotalScanTime
		node.AnalyzeInfo.InsertTime = op.OpStats.TotalInsertTime
		node.AnalyzeInfo.InputBlocks = op.OpStats.TotalInputBlocks
	}

	// 递归处理子操作
	for _, childOp := range op.Children {
		addOpStatsToPlanNodes(childOp, nodes)
	}
}

// 递归遍历 PhyScope 并处理其中的 PhyOperator
func processPhyScope(scope *PhyScope, nodes []*plan.Node) {
	if scope == nil {
		return
	}

	// 处理当前 Scope 中的 Pipeline
	if scope.RootOperator != nil {
		addOpStatsToPlanNodes(scope.RootOperator, nodes)
	}

	// 递归处理前置范围
	for _, preScope := range scope.PreScopes {
		processPhyScope(&preScope, nodes)
	}
}

func (c *Compile) fillPlanNodeAnalyzeInfoV11(plan *PhyPlan) {
	if plan == nil {
		return
	}

	// 处理本地范围
	for _, localScope := range plan.LocalScope {
		processPhyScope(&localScope, c.analV1.qry.Nodes)
	}

	// 处理远程范围
	for _, remoteScope := range plan.RemoteScope {
		processPhyScope(&remoteScope, c.analV1.qry.Nodes)
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

//----------------------------------------------------------------------------------------------------------------------

type PhyPlan struct {
	Version     string     `json:"version"`
	LocalScope  []PhyScope `json:"scope,omitempty"`
	RemoteScope []PhyScope `json:"RemoteScope,omitempty"`
}

type PhyScope struct {
	Magic        magicType     `json:"Magic"`
	Receiver     []PhyReceiver `json:"Receiver,omitempty"`
	DataSource   *PhySource    `json:"DataSource,omitempty"`
	PreScopes    []PhyScope    `json:"PreScopes,omitempty"`
	RootOperator *PhyOperator  `json:"RootOperator,omitempty"`
}

type PhyReceiver struct {
	Idx        int    `json:"Idx"`
	RemoteUuid string `json:"Uuid,omitempty"`
}

type PhySource struct {
	SchemaName   string   `json:"SchemaName"`
	RelationName string   `json:"TableName"`
	Attributes   []string `json:"Columns"`
}

type PhyOperator struct {
	OpName       string                 `json:"OpName"`
	NodeIdx      int                    `json:"NodeIdx"`
	IsFirst      bool                   `json:"IsFirst"`
	IsLast       bool                   `json:"IsLast"`
	DestReceiver []PhyReceiver          `json:"toMergeReceiver,omitempty"`
	OpStats      *process.OperatorStats `json:"OpStats,omitempty"`
	Children     []*PhyOperator         `json:"Children,omitempty"`
}

func PhyPlanToJSON(p PhyPlan) (string, error) {
	jsonData, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func JSONToPhyPlan(jsonStr string) (PhyPlan, error) {
	var p PhyPlan
	err := json.Unmarshal([]byte(jsonStr), &p)
	if err != nil {
		return PhyPlan{}, err
	}
	return p, nil
}

//----------------------------------------------------------------------------------------------------------------------

func ConvertScopeToPhyScope(scope *Scope, receiverMap map[*process.WaitRegister]int) PhyScope {
	phyScope := PhyScope{
		Magic:        scope.Magic,
		DataSource:   ConvertSourceToPhySource(scope.DataSource),
		PreScopes:    []PhyScope{},
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

func getScopeReceiver(s *Scope, rs []*process.WaitRegister, rmp map[*process.WaitRegister]int) []PhyReceiver {
	receivers := make([]PhyReceiver, 0)
	for i := range rs {
		remote := ""
		for _, u := range s.RemoteReceivRegInfos {
			if u.Idx == i {
				remote = fmt.Sprintf("%s", u.Uuid)
				break
			}
		}
		if id, ok := rmp[rs[i]]; ok {
			receivers = append(receivers, PhyReceiver{
				Idx:        id,
				RemoteUuid: remote,
			})
		} else {
			receivers = append(receivers, PhyReceiver{
				Idx:        -1, // "unknown"
				RemoteUuid: remote,
			})
		}
	}
	return receivers
}

// 将 Operator 转换为 PhyOperator
func ConvertOperatorToPhyOperator(op vm.Operator, rmp map[*process.WaitRegister]int) *PhyOperator {
	if op == nil {
		return nil
	}

	phyOp := &PhyOperator{
		OpName:       fmt.Sprintf("%d", op.OpType()),
		NodeIdx:      op.GetOperatorBase().Idx,
		IsFirst:      op.GetOperatorBase().IsFirst,
		IsLast:       op.GetOperatorBase().IsLast,
		DestReceiver: getDestReceiver(op, rmp),
		OpStats:      op.GetOperatorBase().OpAnalyzer.GetOpStats(),
	}

	children := op.GetOperatorBase().Children
	phyChildren := make([]*PhyOperator, len(children))
	for i, child := range children {
		phyChildren[i] = ConvertOperatorToPhyOperator(child, rmp)
	}

	phyOp.Children = phyChildren
	return phyOp
}

// getDestReceiver 返回当前 Operator 的 DestReceiver
func getDestReceiver(op vm.Operator, mp map[*process.WaitRegister]int) []PhyReceiver {
	receivers := make([]PhyReceiver, 0)
	id := op.OpType()
	_, ok := debugInstructionNames[id]
	if ok {
		if id == vm.Connector {
			arg := op.(*connector.Connector)
			if receiverId, okk := mp[arg.Reg]; okk {
				//receivers = append(receivers, receiverId)
				receivers = append(receivers, PhyReceiver{
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
					receivers = append(receivers, PhyReceiver{
						Idx:        receiverId,
						RemoteUuid: "",
					})
				} else {
					//receivers = append(receivers, -1)
					receivers = append(receivers, PhyReceiver{
						Idx:        -1,
						RemoteUuid: "",
					})
				}
			}

			if len(arg.RemoteRegs) != 0 {
				for _, reg := range arg.RemoteRegs {
					receivers = append(receivers, PhyReceiver{
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

func ConvertSourceToPhySource(source *Source) *PhySource {
	if source == nil {
		return nil
	}
	return &PhySource{
		SchemaName:   source.SchemaName,
		RelationName: source.RelationName,
		Attributes:   source.Attributes,
	}
}

func ConvertCompileToPhyPlan(c *Compile) PhyPlan {
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

	phyPlan := PhyPlan{
		Version:     "1.0",        // 假设版本号为1.0，可以根据实际情况调整
		RemoteScope: []PhyScope{}, // 假设这里需要处理 RemoteScope 字段，可以根据实际情况调整
	}

	if len(c.scope) > 0 {
		for i := range c.scope {
			phyScope := ConvertScopeToPhyScope(c.scope[i], receiverMap)
			phyPlan.LocalScope = append(phyPlan.LocalScope, phyScope)
		}
	}

	return phyPlan
}
