// Copyright 2021 Matrix Origin
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

package plan

import (
	"sort"
	"strconv"
	"strings"
)

// ViewDependency is the catalog identity captured while the authoritative View
// schema is generated. Both IDs and names are retained intentionally: COPY
// replacement keeps the logical identity while changing the physical relation,
// and drop/recreate can change both IDs while preserving the qualified name.
type ViewDependency struct {
	AccountID           uint32    `json:"account_id"`
	DatabaseID          uint64    `json:"database_id"`
	RelationID          uint64    `json:"relation_id"`
	LogicalID           uint64    `json:"logical_id"`
	DatabaseName        string    `json:"database_name"`
	RelationName        string    `json:"relation_name"`
	BindingDatabaseName string    `json:"binding_database_name,omitempty"`
	BindingRelationName string    `json:"binding_relation_name,omitempty"`
	RelationKind        string    `json:"relation_kind"`
	Version             uint32    `json:"version"`
	SubscriptionName    string    `json:"subscription_name,omitempty"`
	PublisherAccount    uint32    `json:"publisher_account,omitempty"`
	Snapshot            *Snapshot `json:"snapshot,omitempty"`
	SnapshotName        string    `json:"snapshot_name,omitempty"`
	LowerCaseTableNames int64     `json:"lower_case_table_names,omitempty"`
}

// ViewDependencyIdentityResolver is optional. The frontend implements it
// because only the resolver owns the subscription/snapshot/system-account
// mapping. Planner-only tests and other contexts use the conservative fallback.
type ViewDependencyIdentityResolver interface {
	ResolveViewDependencyAccount(*ObjectRef, *TableDef, *Snapshot) (uint32, error)
}

type viewDependencyScope interface {
	enterNestedView()
	leaveNestedView()
}

type viewDependencyCaptureContext struct {
	CompilerContext
	depth         int
	deps          map[string]ViewDependency
	snapshotNames map[string]string
}

func newViewDependencyCaptureContext(ctx CompilerContext) *viewDependencyCaptureContext {
	return &viewDependencyCaptureContext{
		CompilerContext: ctx,
		deps:            make(map[string]ViewDependency),
		snapshotNames:   make(map[string]string),
	}
}

func (c *viewDependencyCaptureContext) enterNestedView() { c.depth++ }

func (c *viewDependencyCaptureContext) leaveNestedView() {
	if c.depth == 0 {
		panic("unbalanced View dependency capture scope")
	}
	c.depth--
}

func (c *viewDependencyCaptureContext) Resolve(
	schemaName string,
	tableName string,
	snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	obj, tableDef, err := c.CompilerContext.Resolve(schemaName, tableName, snapshot)
	if err == nil && c.depth == 0 && obj != nil && tableDef != nil {
		if err = c.record(obj, tableDef, snapshot, schemaName, tableName); err != nil {
			return nil, nil, err
		}
	}
	return obj, tableDef, err
}

func (c *viewDependencyCaptureContext) ResolveById(
	tableID uint64,
	snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	obj, tableDef, err := c.CompilerContext.ResolveById(tableID, snapshot)
	if err == nil && c.depth == 0 && obj != nil && tableDef != nil {
		if err = c.record(obj, tableDef, snapshot, obj.SchemaName, obj.ObjName); err != nil {
			return nil, nil, err
		}
	}
	return obj, tableDef, err
}

func (c *viewDependencyCaptureContext) ResolveSnapshotWithSnapshotName(
	snapshotName string,
) (*Snapshot, error) {
	snapshot, err := c.CompilerContext.ResolveSnapshotWithSnapshotName(snapshotName)
	if err == nil && snapshot != nil {
		c.snapshotNames[snapshot.String()] = snapshotName
	}
	return snapshot, err
}

func (c *viewDependencyCaptureContext) record(
	obj *ObjectRef,
	tableDef *TableDef,
	snapshot *Snapshot,
	bindingDatabaseName string,
	bindingRelationName string,
) error {
	accountID, err := c.GetAccountId()
	if err != nil {
		return err
	}
	if resolver, ok := c.CompilerContext.(ViewDependencyIdentityResolver); ok {
		accountID, err = resolver.ResolveViewDependencyAccount(obj, tableDef, snapshot)
		if err != nil {
			return err
		}
	} else if obj.PubInfo != nil {
		accountID = uint32(obj.PubInfo.TenantId)
	} else if snapshot != nil && snapshot.Tenant != nil {
		accountID = snapshot.Tenant.TenantID
	}

	databaseName := obj.SchemaName
	if databaseName == "" {
		databaseName = tableDef.DbName
	}
	relationName := obj.ObjName
	if relationName == "" {
		relationName = tableDef.Name
	}
	if bindingDatabaseName == "" {
		bindingDatabaseName = c.DefaultDatabase()
	}
	if bindingRelationName == "" {
		bindingRelationName = relationName
	}
	dependency := ViewDependency{
		AccountID:           accountID,
		DatabaseID:          tableDef.DbId,
		RelationID:          tableDef.TblId,
		LogicalID:           tableDef.LogicalId,
		DatabaseName:        databaseName,
		RelationName:        relationName,
		BindingDatabaseName: bindingDatabaseName,
		BindingRelationName: bindingRelationName,
		RelationKind:        tableDef.TableType,
		Version:             tableDef.Version,
		SubscriptionName:    obj.SubscriptionName,
		Snapshot:            DeepCopySnapshot(snapshot),
		LowerCaseTableNames: c.GetLowerCaseTableNames(),
	}
	if snapshot != nil {
		dependency.SnapshotName = c.snapshotNames[snapshot.String()]
	}
	if obj.PubInfo != nil {
		dependency.PublisherAccount = uint32(obj.PubInfo.TenantId)
	}

	key := viewDependencyKey(dependency)
	c.deps[key] = dependency
	return nil
}

func (c *viewDependencyCaptureContext) dependencies() []ViewDependency {
	dependencies := make([]ViewDependency, 0, len(c.deps))
	for _, dependency := range c.deps {
		dependencies = append(dependencies, dependency)
	}
	sort.Slice(dependencies, func(i, j int) bool {
		return viewDependencyKey(dependencies[i]) < viewDependencyKey(dependencies[j])
	})
	return dependencies
}

func viewDependencyKey(dependency ViewDependency) string {
	bindingDatabaseName := dependency.BindingDatabaseName
	bindingRelationName := dependency.BindingRelationName
	databaseName := dependency.DatabaseName
	relationName := dependency.RelationName
	if dependency.LowerCaseTableNames != 0 {
		bindingDatabaseName = strings.ToLower(bindingDatabaseName)
		bindingRelationName = strings.ToLower(bindingRelationName)
		databaseName = strings.ToLower(databaseName)
		relationName = strings.ToLower(relationName)
	}
	snapshotIdentity := dependency.SnapshotName
	if dependency.Snapshot != nil {
		snapshotIdentity += "\x00" + dependency.Snapshot.String()
	}
	return strings.Join([]string{
		strconv.FormatUint(uint64(dependency.AccountID), 10),
		strconv.FormatUint(dependency.DatabaseID, 10),
		strconv.FormatUint(dependency.RelationID, 10),
		databaseName,
		relationName,
		bindingDatabaseName,
		bindingRelationName,
		dependency.SubscriptionName,
		snapshotIdentity,
	}, "\x00")
}
