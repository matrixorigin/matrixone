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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// SnapshotProvider resolves relations which were opened with the compiling
// transaction. CollectObjectList is explicitly bounded by the same 12-byte
// snapshot carried in TaeRead.
type SnapshotProvider struct {
	Relations map[uint64]engine.Relation
	MPool     *mpool.MPool
	DataDir   string
	TxnOffset int
}

func (p *SnapshotProvider) PrepareSnapshotRead(ctx context.Context, read substrait.Read, snapshot []byte) (substrait.SnapshotFacts, error) {
	var rejected substrait.SnapshotFacts
	if p == nil || p.MPool == nil || len(snapshot) != types.TxnTsSize {
		return rejected, moerr.NewInternalErrorNoCtxf("invalid TAE snapshot provider")
	}
	rel := p.Relations[read.TableID]
	if rel == nil {
		return rejected, moerr.NewInternalErrorNoCtxf("TAE relation %d is not open", read.TableID)
	}
	def := rel.GetTableDef(ctx)
	if def == nil || def.TblId != read.TableID || def.IsTemporary || (def.TableType != "" && def.TableType != "r") {
		rejected.NonTAE = true
		return rejected, nil
	}
	currentSchema, err := substrait.CanonicalSchema(def)
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

	// Any visible delete changes the scanner contract; v1 therefore rejects
	// both row tombstones and tombstone objects instead of approximating them.
	tombstones, err := rel.CollectTombstones(ctx, p.TxnOffset, engine.Policy_CollectAllTombstones)
	if err != nil {
		return rejected, err
	}
	if tombstones != nil && (tombstones.HasAnyInMemoryTombstone() || tombstones.HasAnyTombstoneFile()) {
		rejected.VisibleTombstones = true
		return rejected, nil
	}

	bat := logtailreplay.CreateObjectListBatch()
	defer bat.Clean(p.MPool)
	if err = rel.CollectObjectList(ctx, types.TS{}, ts, bat, p.MPool); err != nil {
		return rejected, err
	}
	stats := make([]objectio.ObjectStats, 0, bat.RowCount())
	isTombstone := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[logtailreplay.ObjectListAttr_IsTombstone_Idx])
	for i := 0; i < bat.RowCount(); i++ {
		if isTombstone[i] {
			rejected.VisibleTombstones = true
			return rejected, nil
		}
		raw := bat.Vecs[logtailreplay.ObjectListAttr_Stats_Idx].GetBytesAt(i)
		if len(raw) != objectio.ObjectStatsLen {
			return rejected, moerr.NewInternalErrorNoCtxf("invalid object stats length %d", len(raw))
		}
		var s objectio.ObjectStats
		s.UnMarshal(raw)
		stats = append(stats, s)
	}

	// With tombstones excluded, StarCount must equal the rows represented by
	// persisted objects. A larger count proves a committed in-memory tail; a
	// smaller/different count is also unsafe and is rejected conservatively.
	var persisted uint64
	for i := range stats {
		persisted += uint64(stats[i].Rows())
	}
	visible, err := rel.StarCount(ctx)
	if err != nil {
		return rejected, err
	}
	if visible != persisted {
		rejected.CommittedInMemory = true
		return rejected, nil
	}
	manifest, objects, err := buildManifest(def, p.DataDir, stats)
	if err != nil {
		return rejected, err
	}
	return substrait.SnapshotFacts{Manifest: manifest, CanonicalSchema: append([]byte(nil), read.Schema...), ObjectNames: objects}, nil
}

type manifest struct {
	Version  int              `json:"version"`
	Database string           `json:"database"`
	Table    string           `json:"table"`
	DataDir  string           `json:"data_dir"`
	Columns  []manifestColumn `json:"columns"`
	Objects  []manifestObject `json:"objects"`
	Stats    manifestStats    `json:"stats"`
}
type manifestColumn struct {
	Name  string `json:"name"`
	OID   int    `json:"oid"`
	Width int    `json:"width,omitempty"`
	Scale int    `json:"scale,omitempty"`
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

func buildManifest(def *planpb.TableDef, dataDir string, stats []objectio.ObjectStats) ([]byte, []string, error) {
	if def == nil {
		return nil, nil, moerr.NewInternalErrorNoCtxf("nil table definition")
	}
	columns := make([]manifestColumn, 0, len(def.Cols))
	for _, c := range def.Cols {
		if c == nil || c.Hidden {
			continue
		}
		columns = append(columns, manifestColumn{Name: c.Name, OID: int(c.Typ.Id), Width: int(c.Typ.Width), Scale: int(c.Typ.Scale)})
	}
	if len(columns) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtxf("table %d has no manifest columns", def.TblId)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].ObjectName().String() < stats[j].ObjectName().String() })
	objects := make([]manifestObject, 0, len(stats))
	names := make([]string, 0, len(stats))
	var rows int64
	var size uint64
	for i := range stats {
		name := stats[i].ObjectName().String()
		if name == "" {
			return nil, nil, moerr.NewInternalErrorNoCtxf("empty object name")
		}
		obj := manifestObject{Path: name, Rows: int64(stats[i].Rows()), Blocks: int64(stats[i].BlkCnt()), Size: uint64(stats[i].Size()), OriginSize: uint64(stats[i].OriginSize())}
		if zm := stats[i].SortKeyZoneMap(); zm.IsInited() {
			obj.ZoneMap = hex.EncodeToString(zm)
		}
		objects = append(objects, obj)
		names = append(names, name)
		rows += obj.Rows
		size += obj.OriginSize
	}
	m := manifest{Version: 1, Database: def.DbName, Table: def.Name, DataDir: dataDir, Columns: columns, Objects: objects, Stats: manifestStats{TotalRows: rows, TotalObjects: len(objects), TotalSize: size}}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, nil, err
	}
	return b, names, nil
}
