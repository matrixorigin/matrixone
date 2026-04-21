// Copyright 2026 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildIvfFlatSecondaryIndexDef(
	ctx CompilerContext,
	indexInfo *tree.Index,
	colMap map[string]*ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*TableDef, error) {
	p, ok := indexplugin.Get(catalog.MoIndexIvfFlatAlgo.ToString())
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("ivfflat plugin not registered")
	}
	return p.Plan().BuildSecondaryIndexDefs(ctx, indexInfo, colMap, existedIndexes, pkeyName)
}
