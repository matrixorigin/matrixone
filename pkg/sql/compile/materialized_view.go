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
	"context"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Finalize all identities in the CREATE transaction before publishing a job.
func (c *Compile) createMaterializedViewDefinition(database engine.Database, target *plan.TableDef) error {
	d, err := mvdefinition.Decode(mvdefinition.PropertyValue(target, mvdefinition.Property), false)
	if err != nil {
		return err
	}
	relation, err := database.Relation(c.proc.Ctx, target.Name, nil)
	if err != nil {
		return err
	}
	d.Target.ID = relation.GetTableID(c.proc.Ctx)
	d.Target.DatabaseID = relation.GetDBID(c.proc.Ctx)
	sources := append([]mvdefinition.Source(nil), d.Sources...)
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Database != sources[j].Database {
			return sources[i].Database < sources[j].Database
		}
		return sources[i].Name < sources[j].Name
	})
	for _, source := range sources {
		if err = lockMoDatabase(c, source.Database, lock.LockMode_Shared); err != nil {
			return err
		}
		if err = lockMoTable(c, source.Database, source.Name, lock.LockMode_Shared); err != nil {
			return err
		}
	}
	if err = d.ValidateSources(func(source mvdefinition.Source) (*plan.TableDef, error) {
		db, err := c.e.Database(c.proc.Ctx, source.Database, c.proc.GetTxnOperator())
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(c.proc.Ctx, source.Name, nil)
		if err != nil {
			return nil, err
		}
		return rel.GetTableDef(c.proc.Ctx), nil
	}); err != nil {
		return err
	}
	if d.State != nil {
		owner := mvdefinition.Owner{Format: mvdefinition.Format, AccountID: d.AccountID, TargetID: d.Target.ID, Generation: d.Generation}
		stateContext := mvdefinition.WithStateCreation(c.proc.Ctx, mvdefinition.StateCreation{Owner: owner, Database: d.State.Database, Name: d.State.Name, ViewSQL: target.GetViewSql().GetView()})
		sql := fmt.Sprintf("CREATE TABLE %s (aggregate_index INT NOT NULL, group_key VARBINARY(65535) NOT NULL, value_key VARBINARY(65535) NOT NULL, ref_count BIGINT NOT NULL, PRIMARY KEY (aggregate_index,group_key,value_key))", sqlquote.QualifiedIdent(d.State.Database, d.State.Name))
		result, err := iscp.ExecWithResult(stateContext, sql, c.proc.GetService(), c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		result.Close()
		state, err := database.Relation(c.proc.Ctx, d.State.Name, nil)
		if err != nil {
			return err
		}
		d.State.ID = state.GetTableID(c.proc.Ctx)
		d.State.DatabaseID = state.GetDBID(c.proc.Ctx)
		owner.StateID = d.State.ID
		if err = setMaterializedViewProperty(c.proc.Ctx, state, mvdefinition.OwnerProperty, mvdefinition.EncodeOwner(owner)); err != nil {
			return err
		}
	}
	if err = d.Validate(true); err != nil {
		return err
	}
	encoded, err := mvdefinition.Encode(d)
	if err != nil {
		return err
	}
	if err = setMaterializedViewProperty(c.proc.Ctx, relation, mvdefinition.Property, encoded); err != nil {
		return err
	}
	if d.Timing == "demand" {
		return nil
	}
	info, err := iscp.MaterializedViewInfo(d)
	if err != nil {
		return err
	}
	spec := &iscp.JobSpec{ConsumerInfo: *info}
	// Target ID is unambiguous even when database/table underscores collide.
	job := &iscp.JobID{DBName: info.SrcTable.DBName, TableName: info.SrcTable.TableName, JobName: fmt.Sprintf("materialized_view_%d", d.Target.ID)}
	_, err = CreateCdcTask(c, spec, job, false)
	return err
}

func setMaterializedViewProperty(ctx context.Context, rel engine.Relation, key, value string) error {
	defs, err := rel.TableDefs(ctx)
	if err != nil {
		return err
	}
	constraint := &engine.ConstraintDef{}
	for _, def := range defs {
		if current, ok := def.(*engine.ConstraintDef); ok {
			data, err := current.MarshalBinary()
			if err != nil {
				return err
			}
			if err = constraint.UnmarshalBinary(data); err != nil {
				return err
			}
			break
		}
	}
	var props *engine.StreamConfigsDef
	for _, item := range constraint.Cts {
		if current, ok := item.(*engine.StreamConfigsDef); ok {
			if props == nil {
				props = current
			}
			// Plan definitions may contribute several property blocks. Replace
			// the key across all of them, so no unresolved CREATE-time envelope
			// survives beside the finalized catalog identity.
			kept := current.Configs[:0]
			for _, property := range current.Configs {
				if property.Key != key {
					kept = append(kept, property)
				}
			}
			current.Configs = kept
		}
	}
	if props == nil {
		props = &engine.StreamConfigsDef{}
		constraint.Cts = append(constraint.Cts, props)
	}
	props.Configs = append(props.Configs, &plan.Property{Key: key, Value: value})
	return rel.UpdateConstraint(ctx, constraint)
}

// Run is called for every EXECUTE, including reuse of a compiled prepared plan.
// A fresh catalog snapshot must not inherit the prior source-generation proof.
func (c *Compile) validateMaterializedViewReads() error {
	if c.pn == nil || c.pn.GetQuery() == nil {
		return nil
	}
	var checked map[uint64]bool
	for _, node := range c.pn.GetQuery().Nodes {
		def := node.GetTableDef()
		if def == nil || !plan2.IsMaterializedViewTableDef(def) || checked[def.TblId] {
			continue
		}
		if checked == nil {
			checked = make(map[uint64]bool)
		}
		checked[def.TblId] = true
		if mvdefinition.CanWrite(c.proc.Ctx, def) {
			continue
		}
		d, err := mvdefinition.FromTable(def)
		if err != nil {
			return err
		}
		info, err := iscp.MaterializedViewInfo(d)
		if err != nil {
			return err
		}
		if _, err = iscp.LoadMaterializedViewDefinition(c.proc.Ctx, c.e, c.proc.GetService(), c.proc.GetTxnOperator(), info, false); err != nil {
			return err
		}
	}
	return nil
}

func requireMaterializedViewCapability(c *Compile) error {
	if !supportsMultiSourceISCP(c.proc.GetService()) {
		return moerr.NewNotSupported(c.proc.Ctx, "materialized view creation requires protocol 64 on all services")
	}
	return nil
}

// This metadata lookup is needed only for unrestricted DELETE candidates.
// Read the owning definitions, including ON DEMAND views which have no ISCP
// job. No source-side count or dependency cache can outlive its target.
func (c *Compile) hasMaterializedViewDependent(accountID uint32, sourceID uint64) (bool, error) {
	result, err := c.runSqlWithResult(fmt.Sprintf("SELECT reldatabase,relname FROM mo_catalog.mo_tables WHERE account_id=%d AND relkind='v'", accountID), int32(accountID))
	if err != nil {
		return false, err
	}
	defer result.Close()
	found := false
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			name := cols[1].GetStringAt(i)
			if mvdefinition.IsStateName(name) {
				continue
			}
			var db engine.Database
			db, err = c.e.Database(c.proc.Ctx, cols[0].GetStringAt(i), c.proc.GetTxnOperator())
			if err != nil {
				return false
			}
			var rel engine.Relation
			rel, err = db.Relation(c.proc.Ctx, name, nil)
			if err != nil {
				return false
			}
			def := rel.GetTableDef(c.proc.Ctx)
			if !plan2.IsMaterializedViewTableDef(def) {
				continue
			}
			var d *mvdefinition.Definition
			d, err = mvdefinition.FromTable(def)
			if err != nil {
				return false
			}
			for _, source := range d.Sources {
				if source.ID == sourceID {
					found = true
					return false
				}
			}
		}
		return true
	})
	return found, err
}
