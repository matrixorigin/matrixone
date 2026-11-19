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

//go:build catalog_candidate_probe

package plan

import (
	"context"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type candidateProbeContext struct{ CompilerContext }

func (candidateProbeContext) GetContext() context.Context { return context.Background() }

// CandidateAlterProbe is a build-tagged diagnostic bridge, not a production API.
func CandidateAlterProbe(source *pb.TableDef, column string) (*pb.TableDef, error) {
	copyDef, err := buildCopyTableDef(context.Background(), source)
	if err != nil {
		return nil, err
	}
	spec := &tree.PrimaryKeyIndex{KeyParts: []*tree.KeyPart{{ColName: tree.NewUnresolvedName(tree.NewCStr(column, 0))}}}
	err = AddPrimaryKey(candidateProbeContext{}, &pb.AlterTable{CopyTableDef: copyDef}, spec, nil)
	return copyDef, err
}
