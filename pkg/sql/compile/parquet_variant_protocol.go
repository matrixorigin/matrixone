// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// The executing reader discovers the file schema after placement. Fence all
// Parquet external scans: neither the format nor the destination type proves
// that a matched file lacks a standard VARIANT group. No footer I/O or
// capability observation is cached by these pure helpers.
func parquetScanRequiresVariantReader(createSQL string) (bool, error) {
	if createSQL == "" {
		return false, nil // Iceberg carries typed tasks rather than ExternParam.
	}
	var param struct{ Format string }
	if err := json.Unmarshal([]byte(createSQL), &param); err != nil {
		return false, err
	}
	return param.Format == tree.PARQUET, nil
}

func parquetQueryRequiresVariantReader(qry *plan.Query) (bool, error) {
	if qry == nil {
		return false, nil
	}
	for _, node := range qry.Nodes {
		if node == nil || node.ExternScan == nil || node.TableDef == nil ||
			node.ExternScan.LoadType == tree.INLINE {
			continue
		}
		// Other external services carry their own SQL/task metadata and reader.
		if node.ExternScan.Type != int32(plan.ExternType_LOAD) &&
			node.ExternScan.Type != int32(plan.ExternType_EXTERNAL_TB) &&
			node.ExternScan.Type != int32(plan.ExternType_RESULT_SCAN) {
			continue
		}
		required, err := parquetScanRequiresVariantReader(node.TableDef.Createsql)
		if err != nil || required {
			return required, err
		}
	}
	return false, nil
}

func parquetPipelineRequiresVariantReader(p *pipeline.Pipeline) (bool, error) {
	if p == nil {
		return false, nil
	}
	for _, instruction := range p.InstructionList {
		if scan := instruction.GetExternalScan(); scan != nil {
			required, err := parquetScanRequiresVariantReader(scan.CreateSql)
			if err != nil || required {
				return required, err
			}
		}
	}
	for _, child := range p.Children {
		required, err := parquetPipelineRequiresVariantReader(child)
		if err != nil || required {
			return required, err
		}
	}
	return false, nil
}
