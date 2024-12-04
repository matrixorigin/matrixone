// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"go.uber.org/zap"
)

type reviseResult struct {
	objs []*catalog.ObjectEntry
	kind taskHostKind
}

type policyGroup struct {
	policies []policy

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func newPolicyGroup(policies ...policy) *policyGroup {
	return &policyGroup{
		policies:       policies,
		configProvider: newCustomConfigProvider(),
	}
}

func (g *policyGroup) onObject(obj *catalog.ObjectEntry) {
	for _, p := range g.policies {
		if p.onObject(obj, g.config) {
			return
		}
	}
}

func (g *policyGroup) revise(rc *resourceController) []reviseResult {
	results := make([]reviseResult, 0, len(g.policies))
	for _, p := range g.policies {
		pResult := p.revise(rc)
		for _, r := range pResult {
			if len(r.objs) > 0 {
				results = append(results, r)
			}
		}
	}
	return results
}

func (g *policyGroup) resetForTable(entry *catalog.TableEntry) {
	g.config = g.configProvider.getConfig(entry)
	for _, p := range g.policies {
		p.resetForTable(entry, g.config)
	}
}

func (g *policyGroup) setConfig(tbl *catalog.TableEntry, txn txnif.AsyncTxn, cfg *BasicPolicyConfig) (err error) {
	if tbl == nil || txn == nil {
		return
	}
	schema := tbl.GetLastestSchema(false)
	ctx := context.Background()
	defer func() {
		if err != nil {
			logutil.Error(
				"Policy-SetConfig-Error",
				zap.Error(err),
				zap.Uint64("table-id", tbl.ID),
				zap.String("table-name", schema.Name),
			)
			txn.Rollback(ctx)
		} else {
			err = txn.Commit(ctx)
			logger := logutil.Info
			if err != nil {
				logger = logutil.Error
			}
			logger(
				"Policy-SetConfig-Commit",
				zap.Error(err),
				zap.String("commit-ts", txn.GetCommitTS().ToString()),
				zap.Uint64("table-id", tbl.ID),
				zap.String("table-name", schema.Name),
			)
			g.configProvider.invalidCache(tbl)
		}
	}()

	moCatalog, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return
	}

	moTables, err := moCatalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		return
	}

	moColumns, err := moCatalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return
	}

	packer := types.NewPacker()
	defer packer.Close()

	packer.Reset()
	packer.EncodeUint32(schema.AcInfo.TenantID)
	packer.EncodeStringType([]byte(tbl.GetDB().GetName()))
	packer.EncodeStringType([]byte(schema.Name))
	pk := packer.Bytes()
	cloned := schema.Clone()
	cloned.Extra.MaxOsizeMergedObj = cfg.MaxOsizeMergedObj
	cloned.Extra.MinOsizeQuailifed = cfg.ObjectMinOsize
	cloned.Extra.MaxObjOnerun = uint32(cfg.MergeMaxOneRun)
	cloned.Extra.Hints = cfg.MergeHints
	err = moTables.UpdateByFilter(ctx, handle.NewEQFilter(pk), uint16(pkgcatalog.MO_TABLES_EXTRA_INFO_IDX), cloned.MustGetExtraBytes(), false)
	if err != nil {
		return
	}

	for _, col := range schema.ColDefs {
		packer.Reset()
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(tbl.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		packer.EncodeStringType([]byte(col.Name))
		pk := packer.Bytes()
		err = moColumns.UpdateByFilter(ctx, handle.NewEQFilter(pk), uint16(pkgcatalog.MO_COLUMNS_ATT_CPKEY_IDX), pk, false)
		if err != nil {
			return
		}
	}

	db, err := txn.GetDatabaseByID(tbl.GetDB().ID)
	if err != nil {
		return
	}
	tblHandle, err := db.GetRelationByID(tbl.ID)
	if err != nil {
		return
	}
	return tblHandle.AlterTable(
		ctx,
		newUpdatePolicyReq(cfg),
	)
}

func (g *policyGroup) getConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	r := g.configProvider.getConfig(tbl)
	if r == nil {
		r = &BasicPolicyConfig{
			ObjectMinOsize:    common.RuntimeOsizeRowsQualified.Load(),
			MaxOsizeMergedObj: common.RuntimeMaxObjOsize.Load(),
			MergeMaxOneRun:    int(common.RuntimeMaxMergeObjN.Load()),
		}
	}
	return r
}
