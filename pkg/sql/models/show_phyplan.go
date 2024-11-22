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

package models

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

const (
	IsFirstMask = 1 << 0 // 0001
	IsLastMask  = 1 << 1 // 0010
)

type ExplainOption int

const (
	NormalOption ExplainOption = iota
	VerboseOption
	AnalyzeOption
)

// ExplainPhyPlan used to `mo_explan_phy` internal function
func ExplainPhyPlan(phy *PhyPlan, statsInfo *statistic.StatsInfo, option ExplainOption) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))
	if len(phy.LocalScope) > 0 || len(phy.RemoteScope) > 0 {
		explainResourceOverview(phy, statsInfo, option, buffer)
	}

	if len(phy.LocalScope) > 0 {
		if option == VerboseOption || option == AnalyzeOption {
			buffer.WriteString("\n")
		}
		buffer.WriteString("LOCAL SCOPES:")
		for i := range phy.LocalScope {
			explainPhyScope(phy.LocalScope[i], i, 0, option, buffer)
		}
	}

	if len(phy.RemoteScope) > 0 {
		buffer.WriteString("\n")
		buffer.WriteString("REMOTE SCOPES:")
		for i := range phy.RemoteScope {
			explainPhyScope(phy.RemoteScope[i], i, 0, option, buffer)
		}
	}
	return buffer.String()
}

func explainResourceOverview(phy *PhyPlan, statsInfo *statistic.StatsInfo, option ExplainOption, buffer *bytes.Buffer) {
	if option == VerboseOption || option == AnalyzeOption {
		gblStats := ExtractPhyPlanGlbStats(phy)
		buffer.WriteString("Overview:\n")
		buffer.WriteString(fmt.Sprintf("\tMemoryUsage:%dB,  DiskI/O:%dB,  NewWorkI/O:%dB,  RetryTime: %v",
			gblStats.MemorySize,
			gblStats.DiskIOSize,
			gblStats.NetWorkSize,
			phy.RetryTime,
		))

		if statsInfo != nil {
			buffer.WriteString("\n")
			// Calculate the total sum of S3 requests for each stage
			list, head, put, get, delete, deleteMul, writtenRows, deletedRows := CalcTotalS3Requests(gblStats, statsInfo)

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
			if option == AnalyzeOption {
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

//----------------------------------------------------------------------------------------------------------------------

func explainPhyScope(scope PhyScope, index int, gap int, option ExplainOption, buffer *bytes.Buffer) {
	gapNextLine(gap, buffer)

	// Scope Header
	receiverStr := getReceiverStr(scope.Receiver)
	buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v, Receiver: %s)", index+1, scope.Magic, scope.Mcpu, receiverStr))

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOperator != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		PrintPipelineTree(scope.RootOperator, prefixStr, true, true, option, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString("  PreScopes: {")
		for i := range scope.PreScopes {
			explainPhyScope(scope.PreScopes[i], i, gap+4, option, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString("  }")
	}
}

func PrintPipelineTree(node *PhyOperator, prefix string, isRoot, isTail bool, option ExplainOption, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	name := node.OpName

	var analyzeStr = ""
	if option == VerboseOption || option == AnalyzeOption {
		// Extract the original bool values
		isFirst := (node.Status & IsFirstMask) != 0
		isLast := (node.Status & IsLastMask) != 0
		analyzeStr = fmt.Sprintf(" (idx:%v, isFirst:%v, isLast:%v)", node.NodeIdx, isFirst, isLast)
	}
	if option == AnalyzeOption && node.OpStats != nil {
		analyzeStr += node.OpStats.String()
	}

	// Write to the current node
	if isRoot {
		headPrefix := "  Pipeline: └── "
		buffer.WriteString(fmt.Sprintf("%s%s%s", headPrefix, name, analyzeStr))
		handleTailNodeReceiver(node, buffer)
		buffer.WriteString("\n")
		// Ensure that child nodes are properly indented
		prefix += "   "
	} else {
		if isTail {
			buffer.WriteString(fmt.Sprintf("%s└── %s%s", prefix, name, analyzeStr))
			handleTailNodeReceiver(node, buffer)
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
	for i := 0; i < len(node.Children); i++ {
		isLast := i == len(node.Children)-1
		PrintPipelineTree(node.Children[i], newPrefix, false, isLast, option, buffer)
	}

	if isRoot {
		trimLastNewline(buffer)
	}
}

// get receiver id string
func getReceiverStr(rsr []PhyReceiver) string {
	str := "["
	for i := range rsr {
		if i != 0 {
			str += ", "
		}
		str += fmt.Sprintf("%d%s", rsr[i].Idx, rsr[i].RemoteUuid)
	}
	str += "]"
	return str
}

// explain the datasource
func showDataSource(source *PhySource) string {
	s := fmt.Sprintf("%s.%s%s", source.SchemaName, source.RelationName, source.Attributes)
	return strings.TrimLeft(s, ".")
}

func gapNextLine(gap int, buffer *bytes.Buffer) {
	buffer.WriteString("\n")
	for i := 0; i < gap; i++ {
		buffer.WriteString(" ")
	}
}

// return n space
func addGap(gap int) string {
	str := ""
	for i := 0; i < gap; i++ {
		str += " "
	}
	return str
}

func handleTailNodeReceiver(node *PhyOperator, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	name := node.OpName
	if name == vm.Connector.String() {
		if len(node.DestReceiver) != 1 {
			panic("The number of DestReceivers of the Connector operator is not 1")
		}
		buffer.WriteString(fmt.Sprintf(" to MergeReceiver %v", node.DestReceiver[0].Idx))
	}

	if name == vm.Dispatch.String() {
		buffer.WriteString(" to Receiver [")
		for i := range node.DestReceiver {
			if i != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(fmt.Sprintf("%v", node.DestReceiver[i].Idx))
		}
		buffer.WriteString("]")
	}
}

func trimLastNewline(buf *bytes.Buffer) {
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		buf.Truncate(len(data) - 1)
	}
}

// GblStats used to hold the total time and wait time of physical plan execution, rename to PhyGblStats
type GblStats struct {
	ScopePrepareTimeConsumed int64
	OperatorTimeConsumed     int64
	MemorySize               int64
	NetWorkSize              int64
	DiskIOSize               int64
	S3ListRequest            int64
	S3HeadRequest            int64
	S3PutRequest             int64
	S3GetRequest             int64
	S3DeleteRequest          int64
	S3DeleteMultiRequest     int64
	WrittenRows              int64
	DeletedRows              int64
}

// CalcTotalS3Requests calculates the total number of S3 requests (List, Head, Put, Get, Delete, DeleteMul)
// by summing up values from global statistics (gblStats) and different stages of the execution process
// (PlanStage, CompileStage, and PrepareRunStage) in the statsInfo.
func CalcTotalS3Requests(gblStats GblStats, statsInfo *statistic.StatsInfo) (list, head, put, get, delete, deleteMul, writtenRows, deletedRows int64) {
	list = gblStats.S3ListRequest + statsInfo.PlanStage.BuildPlanS3Request.List + statsInfo.CompileStage.CompileS3Request.List + statsInfo.PrepareRunStage.ScopePrepareS3Request.List
	head = gblStats.S3HeadRequest + statsInfo.PlanStage.BuildPlanS3Request.Head + statsInfo.CompileStage.CompileS3Request.Head + statsInfo.PrepareRunStage.ScopePrepareS3Request.Head
	put = gblStats.S3PutRequest + statsInfo.PlanStage.BuildPlanS3Request.Put + statsInfo.CompileStage.CompileS3Request.Put + statsInfo.PrepareRunStage.ScopePrepareS3Request.Put
	get = gblStats.S3GetRequest + statsInfo.PlanStage.BuildPlanS3Request.Get + statsInfo.CompileStage.CompileS3Request.Get + statsInfo.PrepareRunStage.ScopePrepareS3Request.Get
	delete = gblStats.S3DeleteRequest + statsInfo.PlanStage.BuildPlanS3Request.Delete + statsInfo.CompileStage.CompileS3Request.Delete + statsInfo.PrepareRunStage.ScopePrepareS3Request.Delete
	deleteMul = gblStats.S3DeleteMultiRequest + statsInfo.PlanStage.BuildPlanS3Request.DeleteMul + statsInfo.CompileStage.CompileS3Request.DeleteMul + statsInfo.PrepareRunStage.ScopePrepareS3Request.DeleteMul
	writtenRows = gblStats.WrittenRows
	deletedRows = gblStats.DeletedRows
	return
}

// Function to recursively process PhyScope and extract stats from PhyOperator
func handlePhyOperator(op *PhyOperator, stats *GblStats) {
	if op == nil {
		return
	}

	// Accumulate stats from the current operator
	if op.OpStats != nil && op.NodeIdx >= 0 {
		stats.OperatorTimeConsumed += op.OpStats.TimeConsumed
		stats.MemorySize += op.OpStats.MemorySize
		stats.NetWorkSize += op.OpStats.NetworkIO
		stats.DiskIOSize += op.OpStats.DiskIO
		stats.S3ListRequest += op.OpStats.S3List
		stats.S3HeadRequest += op.OpStats.S3Head
		stats.S3PutRequest += op.OpStats.S3Put
		stats.S3GetRequest += op.OpStats.S3Get
		stats.S3DeleteRequest += op.OpStats.S3Delete
		stats.S3DeleteMultiRequest += op.OpStats.S3DeleteMul
		stats.WrittenRows += op.OpStats.WrittenRows
		stats.DeletedRows += op.OpStats.DeletedRows
	}

	// Recursively process child operators
	for _, childOp := range op.Children {
		handlePhyOperator(childOp, stats)
	}
}

// Function to process PhyScope (including PreScopes and RootOperator)
func handlePhyScope(scope *PhyScope, stats *GblStats) {
	stats.ScopePrepareTimeConsumed += scope.PrepareTimeConsumed
	// Process the RootOperator of the current scope
	handlePhyOperator(scope.RootOperator, stats)

	// Recursively process PreScopes (which are also PhyScopes)
	for _, preScope := range scope.PreScopes {
		handlePhyScope(&preScope, stats)
	}
}

// Function to process all scopes in a PhyPlan (LocalScope and RemoteScope)
func handlePhyPlanScopes(scopes []PhyScope, stats *GblStats) {
	for _, scope := range scopes {
		handlePhyScope(&scope, stats)
	}
}

// Function to process the entire PhyPlan and extract the stats
func ExtractPhyPlanGlbStats(plan *PhyPlan) GblStats {
	var stats GblStats

	// Process LocalScope
	handlePhyPlanScopes(plan.LocalScope, &stats)

	// Process RemoteScope
	handlePhyPlanScopes(plan.RemoteScope, &stats)

	return stats
}
