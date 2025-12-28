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

	"github.com/matrixorigin/matrixone/pkg/common"
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
		// Format MemoryUsage, SpillSize, DiskI/O, NewWorkI/O, only include non-zero values
		overviewParts := []string{}
		if gblStats.MemorySize > 0 {
			overviewParts = append(overviewParts, fmt.Sprintf("MemoryUsage:%s", common.FormatBytes(gblStats.MemorySize)))
		}
		if gblStats.SpillSize > 0 {
			overviewParts = append(overviewParts, fmt.Sprintf("SpillSize:%s", common.FormatBytes(gblStats.SpillSize)))
		}
		if gblStats.DiskIOSize > 0 {
			overviewParts = append(overviewParts, fmt.Sprintf("DiskI/O:%s", common.FormatBytes(gblStats.DiskIOSize)))
		}
		if gblStats.NetWorkSize > 0 {
			overviewParts = append(overviewParts, fmt.Sprintf("NewWorkI/O:%s", common.FormatBytes(gblStats.NetWorkSize)))
		}
		// Always include RetryTime
		overviewStr := strings.Join(overviewParts, ",  ")
		if len(overviewStr) > 0 {
			buffer.WriteString(fmt.Sprintf("\t%s,  RetryTime: %v", overviewStr, phy.RetryTime))
		} else {
			buffer.WriteString(fmt.Sprintf("\tRetryTime: %v", phy.RetryTime))
		}

		if statsInfo != nil {
			buffer.WriteString("\n")
			// Calculate the total sum of S3 requests for each stage
			list, head, put, get, delete, deleteMul, writtenRows, deletedRows := CalcTotalS3Requests(gblStats, statsInfo)

			s3InputEstByRows := objectio.EstimateS3Input(writtenRows)
			// Format S3 stats, only include non-zero values
			s3OverviewParts := []string{}
			if list > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3List:%d", list))
			}
			if head > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3Head:%d", head))
			}
			if put > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3Put:%d", put))
			}
			if get > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3Get:%d", get))
			}
			if delete > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3Delete:%d", delete))
			}
			if deleteMul > 0 {
				s3OverviewParts = append(s3OverviewParts, fmt.Sprintf("S3DeleteMul:%d", deleteMul))
			}
			// Always include S3InputEstByRows if writtenRows or deletedRows > 0, or if there are S3 requests
			if len(s3OverviewParts) > 0 || writtenRows > 0 || deletedRows > 0 {
				if len(s3OverviewParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t%s, ", strings.Join(s3OverviewParts, ", ")))
				}
				buffer.WriteString(fmt.Sprintf("S3InputEstByRows((%d+%d)/8192):%.4f \n", writtenRows, deletedRows, s3InputEstByRows))
			}

			cpuTimeVal := gblStats.OperatorTimeConsumed +
				int64(statsInfo.ParseStage.ParseDuration+statsInfo.PlanStage.PlanDuration+statsInfo.CompileStage.CompileDuration) +
				statsInfo.PrepareRunStage.ScopePrepareDuration + statsInfo.PrepareRunStage.CompilePreRunOnceDuration -
				statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock - statsInfo.PlanStage.BuildPlanStatsIOConsumption -
				(statsInfo.IOAccessTimeConsumption + statsInfo.S3FSPrefetchFileIOMergerTimeConsumption)

			buffer.WriteString("\tCPU Usage: \n")
			buffer.WriteString(fmt.Sprintf("\t\t- Total CPU Time: %s \n", common.FormatDuration(cpuTimeVal)))
			buffer.WriteString(fmt.Sprintf("\t\t- CPU Time Detail: Parse(%s)+BuildPlan(%s)+Compile(%s)+PhyExec(%s)+PrepareRun(%s)-PreRunWaitLock(%s)-PlanStatsIO(%s)-IOAccess(%s)-IOMerge(%s)\n",
				common.FormatDuration(int64(statsInfo.ParseStage.ParseDuration)),
				common.FormatDuration(int64(statsInfo.PlanStage.PlanDuration)),
				common.FormatDuration(int64(statsInfo.CompileStage.CompileDuration)),
				common.FormatDuration(gblStats.OperatorTimeConsumed),
				common.FormatDuration(gblStats.ScopePrepareTimeConsumed+statsInfo.PrepareRunStage.CompilePreRunOnceDuration),
				common.FormatDuration(int64(statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock)),
				common.FormatDuration(int64(statsInfo.PlanStage.BuildPlanStatsIOConsumption)),
				common.FormatDuration(int64(statsInfo.IOAccessTimeConsumption)),
				common.FormatDuration(int64(statsInfo.S3FSPrefetchFileIOMergerTimeConsumption))))
			buffer.WriteString(fmt.Sprintf("\t\t- Permission Authentication Stats Array: %v \n", statsInfo.PermissionAuth))

			//-------------------------------------------------------------------------------------------------------
			if option == AnalyzeOption {
				buffer.WriteString("\tQuery Build Plan Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %s \n", common.FormatDuration(int64(statsInfo.PlanStage.PlanDuration)-statsInfo.PlanStage.BuildPlanStatsIOConsumption)))

				// Format S3 request stats, only include non-zero values
				s3ReqParts := []string{}
				if statsInfo.PlanStage.BuildPlanS3Request.List > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3List:%d", statsInfo.PlanStage.BuildPlanS3Request.List))
				}
				if statsInfo.PlanStage.BuildPlanS3Request.Head > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3Head:%d", statsInfo.PlanStage.BuildPlanS3Request.Head))
				}
				if statsInfo.PlanStage.BuildPlanS3Request.Put > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3Put:%d", statsInfo.PlanStage.BuildPlanS3Request.Put))
				}
				if statsInfo.PlanStage.BuildPlanS3Request.Get > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3Get:%d", statsInfo.PlanStage.BuildPlanS3Request.Get))
				}
				if statsInfo.PlanStage.BuildPlanS3Request.Delete > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3Delete:%d", statsInfo.PlanStage.BuildPlanS3Request.Delete))
				}
				if statsInfo.PlanStage.BuildPlanS3Request.DeleteMul > 0 {
					s3ReqParts = append(s3ReqParts, fmt.Sprintf("S3DeleteMul:%d", statsInfo.PlanStage.BuildPlanS3Request.DeleteMul))
				}
				// Only print S3 request line if at least one value is non-zero
				if len(s3ReqParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- %s\n", strings.Join(s3ReqParts, ", ")))
				}

				buffer.WriteString(fmt.Sprintf("\t\t- Build Plan Duration: %s \n", common.FormatDuration(int64(statsInfo.PlanStage.PlanDuration))))
				buffer.WriteString(fmt.Sprintf("\t\t- Call Stats Duration: %s \n", common.FormatDuration(int64(statsInfo.PlanStage.BuildPlanStatsDuration))))
				buffer.WriteString(fmt.Sprintf("\t\t- Call StatsInCache Duration: %s \n", common.FormatDuration(int64(statsInfo.PlanStage.BuildPlanStatsInCacheDuration))))
				buffer.WriteString(fmt.Sprintf("\t\t- Call Stats IO Consumption: %s \n", common.FormatDuration(int64(statsInfo.PlanStage.BuildPlanStatsIOConsumption))))

				// Format Call Stats S3, only include non-zero values
				s3StatsParts := []string{}
				if statsInfo.PlanStage.BuildPlanStatsS3.List > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3List:%d", statsInfo.PlanStage.BuildPlanStatsS3.List))
				}
				if statsInfo.PlanStage.BuildPlanStatsS3.Head > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3Head:%d", statsInfo.PlanStage.BuildPlanStatsS3.Head))
				}
				if statsInfo.PlanStage.BuildPlanStatsS3.Put > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3Put:%d", statsInfo.PlanStage.BuildPlanStatsS3.Put))
				}
				if statsInfo.PlanStage.BuildPlanStatsS3.Get > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3Get:%d", statsInfo.PlanStage.BuildPlanStatsS3.Get))
				}
				if statsInfo.PlanStage.BuildPlanStatsS3.Delete > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3Delete:%d", statsInfo.PlanStage.BuildPlanStatsS3.Delete))
				}
				if statsInfo.PlanStage.BuildPlanStatsS3.DeleteMul > 0 {
					s3StatsParts = append(s3StatsParts, fmt.Sprintf("S3DeleteMul:%d", statsInfo.PlanStage.BuildPlanStatsS3.DeleteMul))
				}
				// Only print Call Stats S3 line if at least one value is non-zero
				if len(s3StatsParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- Call Stats %s\n", strings.Join(s3StatsParts, ", ")))
				}

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Compile Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %s \n", common.FormatDuration(int64(statsInfo.CompileStage.CompileDuration))))

				// Format S3 request stats, only include non-zero values
				s3CompileParts := []string{}
				if statsInfo.CompileStage.CompileS3Request.List > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3List:%d", statsInfo.CompileStage.CompileS3Request.List))
				}
				if statsInfo.CompileStage.CompileS3Request.Head > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3Head:%d", statsInfo.CompileStage.CompileS3Request.Head))
				}
				if statsInfo.CompileStage.CompileS3Request.Put > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3Put:%d", statsInfo.CompileStage.CompileS3Request.Put))
				}
				if statsInfo.CompileStage.CompileS3Request.Get > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3Get:%d", statsInfo.CompileStage.CompileS3Request.Get))
				}
				if statsInfo.CompileStage.CompileS3Request.Delete > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3Delete:%d", statsInfo.CompileStage.CompileS3Request.Delete))
				}
				if statsInfo.CompileStage.CompileS3Request.DeleteMul > 0 {
					s3CompileParts = append(s3CompileParts, fmt.Sprintf("S3DeleteMul:%d", statsInfo.CompileStage.CompileS3Request.DeleteMul))
				}
				// Only print S3 request line if at least one value is non-zero
				if len(s3CompileParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- %s\n", strings.Join(s3CompileParts, ", ")))
				}
				buffer.WriteString(fmt.Sprintf("\t\t- Compile TableScan Duration: %s \n", common.FormatDuration(int64(statsInfo.CompileStage.CompileTableScanDuration))))

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Prepare Exec Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %s \n", common.FormatDuration(gblStats.ScopePrepareTimeConsumed+statsInfo.PrepareRunStage.CompilePreRunOnceDuration-statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock)))
				buffer.WriteString(fmt.Sprintf("\t\t- CompilePreRunOnce Duration: %s \n", common.FormatDuration(int64(statsInfo.PrepareRunStage.CompilePreRunOnceDuration))))
				buffer.WriteString(fmt.Sprintf("\t\t- PreRunOnce WaitLock: %s \n", common.FormatDuration(int64(statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock))))
				buffer.WriteString(fmt.Sprintf("\t\t- ScopePrepareTimeConsumed: %s \n", common.FormatDuration(gblStats.ScopePrepareTimeConsumed)))
				buffer.WriteString(fmt.Sprintf("\t\t- BuildReader Duration: %s \n", common.FormatDuration(int64(statsInfo.PrepareRunStage.BuildReaderDuration))))

				// Format S3 request stats, only include non-zero values
				s3PrepareParts := []string{}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.List > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3List:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.List))
				}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.Head > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3Head:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.Head))
				}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.Put > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3Put:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.Put))
				}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.Get > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3Get:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.Get))
				}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.Delete > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3Delete:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.Delete))
				}
				if statsInfo.PrepareRunStage.ScopePrepareS3Request.DeleteMul > 0 {
					s3PrepareParts = append(s3PrepareParts, fmt.Sprintf("S3DeleteMul:%d", statsInfo.PrepareRunStage.ScopePrepareS3Request.DeleteMul))
				}
				// Only print S3 request line if at least one value is non-zero
				if len(s3PrepareParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- %s\n", strings.Join(s3PrepareParts, ", ")))
				}

				//-------------------------------------------------------------------------------------------------------
				buffer.WriteString("\tQuery Execution Stage:\n")
				buffer.WriteString(fmt.Sprintf("\t\t- CPU Time: %s \n", common.FormatDuration(gblStats.OperatorTimeConsumed)))

				// Format S3 request stats, only include non-zero values
				s3ExecParts := []string{}
				if gblStats.S3ListRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3List:%d", gblStats.S3ListRequest))
				}
				if gblStats.S3HeadRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3Head:%d", gblStats.S3HeadRequest))
				}
				if gblStats.S3PutRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3Put:%d", gblStats.S3PutRequest))
				}
				if gblStats.S3GetRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3Get:%d", gblStats.S3GetRequest))
				}
				if gblStats.S3DeleteRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3Delete:%d", gblStats.S3DeleteRequest))
				}
				if gblStats.S3DeleteMultiRequest > 0 {
					s3ExecParts = append(s3ExecParts, fmt.Sprintf("S3DeleteMul:%d", gblStats.S3DeleteMultiRequest))
				}
				// Only print S3 request line if at least one value is non-zero
				if len(s3ExecParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- %s\n", strings.Join(s3ExecParts, ", ")))
				}

				// Format MemoryUsage, SpillSize, DiskI/O, NewWorkI/O, only include non-zero values
				resourceParts := []string{}
				if gblStats.MemorySize > 0 {
					resourceParts = append(resourceParts, fmt.Sprintf("MemoryUsage: %s", common.FormatBytes(gblStats.MemorySize)))
				}
				if gblStats.SpillSize > 0 {
					resourceParts = append(resourceParts, fmt.Sprintf("SpillSize: %s", common.FormatBytes(gblStats.SpillSize)))
				}
				if gblStats.DiskIOSize > 0 {
					resourceParts = append(resourceParts, fmt.Sprintf("DiskI/O: %s", common.FormatBytes(gblStats.DiskIOSize)))
				}
				if gblStats.NetWorkSize > 0 {
					resourceParts = append(resourceParts, fmt.Sprintf("NewWorkI/O: %s", common.FormatBytes(gblStats.NetWorkSize)))
				}
				// Only print resource line if at least one value is non-zero
				if len(resourceParts) > 0 {
					buffer.WriteString(fmt.Sprintf("\t\t- %s\n", strings.Join(resourceParts, ",  ")))
				}
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
	if len(scope.Receiver) > 0 {
		receiverStr := getReceiverStr(scope.Receiver)
		buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v, Receiver: %s)", index+1, scope.Magic, scope.Mcpu, receiverStr))
	} else {
		buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v)", index+1, scope.Magic, scope.Mcpu))
	}

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
		// Always output the closing brace on a new line with proper indentation
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
		analyzeStr = fmt.Sprintf(" (%v,%v,%v)", node.NodeIdx, isFirst, isLast)
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
	SpillSize                int64
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
		stats.SpillSize += op.OpStats.SpillSize
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
// ExplainPhyPlanOverview generates only the Overview section of the physical plan
func ExplainPhyPlanOverview(phy *PhyPlan, statsInfo *statistic.StatsInfo, option ExplainOption) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))
	if len(phy.LocalScope) > 0 || len(phy.RemoteScope) > 0 {
		explainResourceOverview(phy, statsInfo, option, buffer)
	}
	return buffer.String()
}

// ExplainPhyPlanCompressed generates a compressed version with key information but reduced verbosity
func ExplainPhyPlanCompressed(phy *PhyPlan, statsInfo *statistic.StatsInfo, option ExplainOption) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	
	// Always include Overview
	if len(phy.LocalScope) > 0 || len(phy.RemoteScope) > 0 {
		explainResourceOverview(phy, statsInfo, option, buffer)
	}
	
	buffer.WriteString("\nPhysical Plan Deployment (Compressed):\n")
	
	if len(phy.LocalScope) > 0 {
		buffer.WriteString("LOCAL SCOPES:\n")
		for i, scope := range phy.LocalScope {
			explainPhyScopeCompressed(scope, i, 0, buffer)
		}
	}
	
	if len(phy.RemoteScope) > 0 {
		buffer.WriteString("REMOTE SCOPES:\n")
		for i, scope := range phy.RemoteScope {
			explainPhyScopeCompressed(scope, i, 0, buffer)
		}
	}
	
	return buffer.String()
}

// explainPhyScopeCompressed generates compressed scope information with key metrics only
func explainPhyScopeCompressed(scope PhyScope, scopeIdx int, depth int, buffer *bytes.Buffer) {
	indent := strings.Repeat("  ", depth)
	
	// Scope header with key info
	buffer.WriteString(fmt.Sprintf("%sScope %d (%s, mcpu: %d", indent, scopeIdx+1, scope.Magic, scope.Mcpu))
	if len(scope.Receiver) > 0 {
		buffer.WriteString(fmt.Sprintf(", Receiver: %v", scope.Receiver))
	}
	buffer.WriteString(")\n")
	
	// DataSource if present
	if scope.DataSource != nil && scope.DataSource.SchemaName != "" {
		buffer.WriteString(fmt.Sprintf("%s  DataSource: %s.%s%s\n", 
			indent, scope.DataSource.SchemaName, scope.DataSource.RelationName, 
			formatColumns(scope.DataSource.Attributes)))
	}
	
	// Pipeline - only show operators with significant metrics
	if scope.RootOperator != nil {
		buffer.WriteString(fmt.Sprintf("%s  Pipeline: ", indent))
		explainOperatorChainCompressed(scope.RootOperator, buffer)
		buffer.WriteString("\n")
	}
	
	// PreScopes - recursively but more compact
	if len(scope.PreScopes) > 0 {
		buffer.WriteString(fmt.Sprintf("%s  PreScopes: %d scope(s)\n", indent, len(scope.PreScopes)))
		for i, preScope := range scope.PreScopes {
			explainPhyScopeCompressed(preScope, i, depth+2, buffer)
		}
	}
}

// explainOperatorChainCompressed shows operator chain with only key metrics
func explainOperatorChainCompressed(op *PhyOperator, buffer *bytes.Buffer) {
	if op == nil {
		return
	}
	
	// Show operator with key metrics only
	metrics := ""
	if op.OpStats != nil {
		stats := op.OpStats
		// Only show non-zero significant metrics
		parts := []string{}
		if stats.CallNum > 0 {
			parts = append(parts, fmt.Sprintf("Calls:%d", stats.CallNum))
		}
		if stats.TimeConsumed > 1000000 { // > 1ms
			parts = append(parts, fmt.Sprintf("Time:%s", common.FormatDuration(stats.TimeConsumed)))
		}
		if stats.InputRows > 0 {
			parts = append(parts, fmt.Sprintf("Rows:%d→%d", stats.InputRows, stats.OutputRows))
		}
		if stats.InputSize > 1024 { // > 1KB
			parts = append(parts, fmt.Sprintf("Size:%s", common.FormatBytes(stats.InputSize)))
		}
		if len(parts) > 0 {
			metrics = " [" + strings.Join(parts, " ") + "]"
		}
	}
	
	buffer.WriteString(fmt.Sprintf("%s%s", op.OpName, metrics))
	
	// Show child operators in chain
	if len(op.Children) > 0 {
		buffer.WriteString(" → ")
		explainOperatorChainCompressed(op.Children[0], buffer)
	}
}

// formatColumns formats column list compactly
func formatColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	if len(columns) <= 3 {
		return "[" + strings.Join(columns, " ") + "]"
	}
	return fmt.Sprintf("[%s...+%d]", strings.Join(columns[:2], " "), len(columns)-2)
}
