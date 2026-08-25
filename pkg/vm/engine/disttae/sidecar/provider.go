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

package sidecar

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// SnapshotProvider resolves relations which were opened with the compiling
// transaction. Object visitation is explicitly bounded by the same 12-byte
// snapshot carried in TaeRead.
type SnapshotProvider struct {
	Relations map[uint64]engine.Relation
	MPool     *mpool.MPool
	DataDir   string
	ReadView  client.WorkspaceReadView
}

func (p *SnapshotProvider) PrepareSnapshotRead(ctx context.Context, read substrait.Read, snapshot []byte) (substrait.SnapshotFacts, error) {
	return p.prepareSnapshotRead(ctx, read, snapshot, substrait.MaxManifestBytes)
}

func (p *SnapshotProvider) prepareSnapshotRead(ctx context.Context, read substrait.Read, snapshot []byte, maximum int) (substrait.SnapshotFacts, error) {
	var rejected substrait.SnapshotFacts
	if p == nil || p.MPool == nil || len(snapshot) != types.TxnTsSize || maximum <= 0 || maximum > substrait.MaxManifestBytes {
		return rejected, moerr.NewInternalErrorNoCtxf("invalid TAE snapshot provider")
	}
	rel := p.Relations[read.TableID]
	if rel == nil {
		return rejected, moerr.NewInternalErrorNoCtxf("TAE relation %d is not open", read.TableID)
	}
	if partitioned, ok := rel.(interface{ IsPartitionedRelation() bool }); ok && partitioned.IsPartitionedRelation() {
		rejected.NonTAE = true
		return rejected, nil
	}
	locality, ok := rel.(interface{ CanVisitSnapshotLocally() (bool, error) })
	if !ok {
		rejected.NonTAE = true
		return rejected, nil
	}
	local, err := locality.CanVisitSnapshotLocally()
	if err != nil {
		return rejected, err
	}
	if !local {
		rejected.NonTAE = true
		return rejected, nil
	}
	def := rel.GetTableDef(ctx)
	if def == nil || def.DbId != read.DatabaseID || def.TblId != read.TableID || def.IsTemporary || (def.TableType != "" && def.TableType != "r") {
		rejected.NonTAE = true
		return rejected, nil
	}
	if def.Version != read.SchemaVersion {
		return rejected, moerr.NewInternalErrorNoCtxf("table %d physical schema changed after planning", read.TableID)
	}
	plannedDef, ok := projectPlannedColumns(def, read.Columns)
	if !ok {
		return rejected, moerr.NewInternalErrorNoCtxf("table %d physical schema changed after planning", read.TableID)
	}
	currentSchema, err := substrait.CanonicalSchema(plannedDef)
	if err != nil {
		return rejected, err
	}
	if !bytes.Equal(currentSchema, read.Schema) {
		return rejected, moerr.NewInternalErrorNoCtxf("table %d schema changed after planning", read.TableID)
	}
	var ts types.TS
	if err := ts.Unmarshal(snapshot); err != nil {
		return rejected, err
	}

	collector, ok := rel.(interface {
		VisitSnapshotObjects(context.Context, types.TS, func(objectio.ObjectStats, bool) error) error
	})
	if !ok {
		rejected.NonTAE = true
		return rejected, nil
	}

	tombstoneChecker, ok := rel.(interface {
		HasSnapshotTombstones(context.Context, client.WorkspaceReadView, types.TS) (bool, error)
	})
	if !ok {
		rejected.NonTAE = true
		return rejected, nil
	}
	// Any visible delete changes the scanner contract; v1 therefore rejects
	// both row tombstones and tombstone objects. The presence probe must stop at
	// the first match instead of materializing and sorting the delete set.
	hasTombstones, err := tombstoneChecker.HasSnapshotTombstones(ctx, p.ReadView, ts)
	if err != nil {
		return rejected, err
	}
	if hasTombstones {
		rejected.VisibleTombstones = true
		return rejected, nil
	}

	builder, err := newManifestBuilder(def, read.AccountID, read.DatabaseID, p.DataDir, maximum)
	if err != nil {
		return rejected, err
	}
	err = collector.VisitSnapshotObjects(ctx, ts, func(stats objectio.ObjectStats, isTombstone bool) error {
		if isTombstone {
			rejected.VisibleTombstones = true
			return moerr.NewInternalErrorNoCtx("sidecar snapshot rejected")
		}
		if stats.GetAppendable() {
			rejected.Uncommitted = true
			return moerr.NewInternalErrorNoCtx("sidecar snapshot rejected")
		}
		return builder.add(stats)
	})
	if rejected.VisibleTombstones || rejected.Uncommitted {
		return rejected, nil
	}
	if err != nil {
		return rejected, err
	}

	// With tombstones excluded, StarCount must equal the rows represented by
	// persisted objects. A larger count proves a committed in-memory tail; a
	// smaller/different count is also unsafe and is rejected conservatively.
	visible, err := rel.StarCount(ctx)
	if err != nil {
		return rejected, err
	}
	if visible != builder.rows {
		rejected.CommittedInMemory = true
		return rejected, nil
	}
	manifest, objects, err := builder.finish()
	if err != nil {
		return rejected, err
	}
	return substrait.SnapshotFacts{Manifest: manifest, CanonicalSchema: append([]byte(nil), read.Schema...), ObjectNames: objects}, nil
}

type manifest struct {
	Version       int              `json:"version"`
	AccountID     uint64           `json:"account_id"`
	DatabaseID    uint64           `json:"database_id"`
	TableID       uint64           `json:"table_id"`
	SchemaVersion uint32           `json:"schema_version"`
	Database      string           `json:"database"`
	Table         string           `json:"table"`
	DataDir       string           `json:"data_dir"`
	Columns       []manifestColumn `json:"columns"`
	Objects       []manifestObject `json:"objects"`
	Stats         manifestStats    `json:"stats"`
}
type manifestColumn struct {
	Name           string `json:"name"`
	ColumnID       uint64 `json:"column_id"`
	SequenceNumber uint32 `json:"sequence_number"`
	OID            int    `json:"oid"`
	Width          int    `json:"width,omitempty"`
	Scale          int    `json:"scale,omitempty"`
}
type manifestObject struct {
	Path       string `json:"path"`
	Rows       int64  `json:"rows"`
	Blocks     int64  `json:"blocks"`
	Size       uint64 `json:"size,omitempty"`
	OriginSize uint64 `json:"origin_size,omitempty"`
	ZoneMap    string `json:"zone_map,omitempty"`
}
type manifestStats struct {
	TotalRows    int64  `json:"total_rows"`
	TotalObjects int    `json:"total_objects"`
	TotalSize    uint64 `json:"total_origin_size"`
}

type manifestBuilder struct {
	manifest    manifest
	maximum     int
	emptySize   int
	objectBytes int
	rows        uint64
}

func newManifestBuilder(def *planpb.TableDef, accountID, databaseID uint64, dataDir string, maximum int) (*manifestBuilder, error) {
	if def == nil {
		return nil, moerr.NewInternalErrorNoCtxf("nil table definition")
	}
	if databaseID == 0 || def.TblId == 0 || maximum <= 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid manifest identity or size bound")
	}
	columns := make([]manifestColumn, 0, len(def.Cols))
	for _, c := range def.Cols {
		if c == nil || c.Hidden {
			continue
		}
		columns = append(columns, manifestColumn{Name: c.Name, ColumnID: c.ColId, SequenceNumber: c.Seqnum, OID: int(c.Typ.Id), Width: int(c.Typ.Width), Scale: int(c.Typ.Scale)})
	}
	if len(columns) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("table %d has no manifest columns", def.TblId)
	}
	m := manifest{Version: 2, AccountID: accountID, DatabaseID: databaseID, TableID: def.TblId, SchemaVersion: def.Version, Database: def.DbName, Table: def.Name, DataDir: dataDir, Columns: columns, Objects: make([]manifestObject, 0)}
	empty, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if len(empty) > maximum {
		return nil, substrait.NotEligible(substrait.EligibilitySnapshot, fmt.Sprintf("manifest metadata is %d bytes, maximum is %d", len(empty), maximum))
	}
	return &manifestBuilder{manifest: m, maximum: maximum, emptySize: len(empty)}, nil
}

func (b *manifestBuilder) add(stats objectio.ObjectStats) error {
	name := stats.ObjectName().String()
	if name == "" {
		return moerr.NewInternalErrorNoCtxf("empty object name")
	}
	obj := manifestObject{Path: name, Rows: int64(stats.Rows()), Blocks: int64(stats.BlkCnt()), Size: uint64(stats.Size()), OriginSize: uint64(stats.OriginSize())}
	if zm := stats.SortKeyZoneMap(); zm.IsInited() {
		obj.ZoneMap = hex.EncodeToString(zm)
	}
	if obj.Rows < 0 || b.manifest.Stats.TotalRows > math.MaxInt64-obj.Rows || math.MaxUint64-b.manifest.Stats.TotalSize < obj.OriginSize {
		return moerr.NewInternalErrorNoCtx("manifest statistics overflow")
	}
	encoded, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	nextRows := b.manifest.Stats.TotalRows + obj.Rows
	nextObjects := len(b.manifest.Objects) + 1
	nextSize := b.manifest.Stats.TotalSize + obj.OriginSize
	nextObjectBytes := b.objectBytes + len(encoded)
	if nextObjects > 1 {
		nextObjectBytes++
	}
	projected := b.emptySize - len("[]") + nextObjectBytes + decimalGrowth(uint64(nextRows)) + decimalGrowth(uint64(nextObjects)) + decimalGrowth(nextSize)
	if projected > b.maximum {
		return substrait.NotEligible(substrait.EligibilitySnapshot, fmt.Sprintf("manifest exceeds maximum of %d bytes", b.maximum))
	}
	b.manifest.Objects = append(b.manifest.Objects, obj)
	b.manifest.Stats = manifestStats{TotalRows: nextRows, TotalObjects: nextObjects, TotalSize: nextSize}
	b.objectBytes = nextObjectBytes
	b.rows += uint64(obj.Rows)
	return nil
}

func (b *manifestBuilder) finish() ([]byte, []string, error) {
	sort.Slice(b.manifest.Objects, func(i, j int) bool { return b.manifest.Objects[i].Path < b.manifest.Objects[j].Path })
	names := make([]string, len(b.manifest.Objects))
	for i := range b.manifest.Objects {
		names[i] = b.manifest.Objects[i].Path
	}
	encoded, err := json.Marshal(b.manifest)
	if err != nil {
		return nil, nil, err
	}
	if len(encoded) > b.maximum {
		return nil, nil, substrait.NotEligible(substrait.EligibilitySnapshot, fmt.Sprintf("manifest is %d bytes, maximum is %d", len(encoded), b.maximum))
	}
	return encoded, names, nil
}

func decimalGrowth(value uint64) int {
	digits := 1
	for value >= 10 {
		value /= 10
		digits++
	}
	return digits - 1
}

func projectPlannedColumns(def *planpb.TableDef, planned []substrait.ColumnMapping) (*planpb.TableDef, bool) {
	if def == nil || len(planned) == 0 {
		return nil, false
	}
	byID := make(map[uint64]*planpb.ColDef, len(def.Cols))
	for _, column := range def.Cols {
		if column == nil {
			return nil, false
		}
		if column.Hidden {
			continue
		}
		if _, exists := byID[column.ColId]; exists {
			return nil, false
		}
		byID[column.ColId] = column
	}
	columns := make([]*planpb.ColDef, 0, len(planned))
	seen := make(map[uint64]struct{}, len(planned))
	for _, mapping := range planned {
		column := byID[mapping.ColumnID]
		if column == nil || column.Seqnum != mapping.SequenceNumber {
			return nil, false
		}
		if _, duplicate := seen[mapping.ColumnID]; duplicate {
			return nil, false
		}
		seen[mapping.ColumnID] = struct{}{}
		columns = append(columns, column)
	}
	projected := *def
	projected.Cols = columns
	return &projected, true
}

func buildManifest(def *planpb.TableDef, accountID, databaseID uint64, dataDir string, stats []objectio.ObjectStats) ([]byte, []string, error) {
	builder, err := newManifestBuilder(def, accountID, databaseID, dataDir, substrait.MaxManifestBytes)
	if err != nil {
		return nil, nil, err
	}
	for i := range stats {
		if err := builder.add(stats[i]); err != nil {
			return nil, nil, err
		}
	}
	return builder.finish()
}
