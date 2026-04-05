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

package rpc

import (
	"encoding/hex"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// Manifest is the JSON structure consumed by the DuckDB TAE scanner extension.
// See duckdb_tae_scanner/DESIGN.md §13.3 for the full specification.
type Manifest struct {
	Version    int              `json:"version"`
	Database   string           `json:"database"`
	Table      string           `json:"table"`
	DataDir    string           `json:"data_dir"`
	SortColumn string           `json:"sort_column,omitempty"`
	Columns    []ManifestColumn `json:"columns"`
	Objects    []ManifestObject `json:"objects"`
	Stats      *ManifestStats   `json:"stats,omitempty"`
}

type ManifestColumn struct {
	Name  string `json:"name"`
	OID   int    `json:"oid"`
	Width int    `json:"width,omitempty"`
	Scale int    `json:"scale,omitempty"`
}

type ManifestObject struct {
	Path       string `json:"path"`
	Rows       int64  `json:"rows"`
	Blocks     int64  `json:"blocks"`
	Size       uint64 `json:"size,omitempty"`
	OriginSize uint64 `json:"origin_size,omitempty"`
	ZoneMap    string `json:"zone_map,omitempty"`
}

type ManifestStats struct {
	TotalRows    int64  `json:"total_rows"`
	TotalObjects int    `json:"total_objects"`
	TotalSize    uint64 `json:"total_origin_size"`
}

// GenerateManifest serializes a table's catalog state into a manifest JSON
// that the DuckDB TAE scanner can read. It uses the current catalog snapshot
// (all committed objects visible now) without requiring a user transaction.
//
// dataDir is the base directory where .tae object files are stored
// (e.g., the fileservice shared path).
func GenerateManifest(table *catalog.TableEntry, dataDir string) ([]byte, error) {
	schema := table.GetLastestSchema(false)

	// Build column list, skipping hidden/internal columns
	columns := make([]ManifestColumn, 0, len(schema.ColDefs))
	var sortColumn string
	for _, col := range schema.ColDefs {
		if col.IsHidden() || col.IsPhyAddr() {
			continue
		}
		mc := ManifestColumn{
			Name: col.Name,
			OID:  int(col.Type.Oid),
		}
		if col.Type.Width > 0 {
			mc.Width = int(col.Type.Width)
		}
		if col.Type.Scale > 0 {
			mc.Scale = int(col.Type.Scale)
		}
		columns = append(columns, mc)
	}

	// Detect sort key column
	if schema.HasSortKey() {
		sortCol := schema.GetSingleSortKey()
		if !sortCol.IsHidden() {
			sortColumn = sortCol.Name
		}
	}

	// Enumerate visible objects using a mock "now" transaction
	// (sees all committed objects, not MVCC-bound to a user txn)
	readTxn := txnbase.MockTxnReaderWithNow()
	it := table.MakeDataVisibleObjectIt(readTxn)
	defer it.Release()

	objects := make([]ManifestObject, 0)
	var totalRows int64
	var totalSize uint64

	for it.Next() {
		entry := it.Item()
		stats := &entry.ObjectStats

		obj := ManifestObject{
			Path:       stats.ObjectName().String(),
			Rows:       int64(stats.Rows()),
			Blocks:     int64(stats.BlkCnt()),
			Size:       uint64(stats.Size()),
			OriginSize: uint64(stats.OriginSize()),
		}

		// Include sort key zone map if non-empty
		zm := stats.SortKeyZoneMap()
		if zm.IsInited() {
			obj.ZoneMap = hex.EncodeToString(zm)
		}

		objects = append(objects, obj)
		totalRows += obj.Rows
		totalSize += obj.OriginSize
	}

	manifest := Manifest{
		Version:    1,
		Database:   table.GetDB().GetName(),
		Table:      schema.Name,
		DataDir:    dataDir,
		SortColumn: sortColumn,
		Columns:    columns,
		Objects:    objects,
		Stats: &ManifestStats{
			TotalRows:    totalRows,
			TotalObjects: len(objects),
			TotalSize:    totalSize,
		},
	}

	return json.Marshal(manifest)
}

// GenerateManifestPretty is like GenerateManifest but produces indented JSON.
func GenerateManifestPretty(table *catalog.TableEntry, dataDir string) ([]byte, error) {
	data, err := GenerateManifest(table, dataDir)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return json.MarshalIndent(m, "", "  ")
}

// GenerateManifestForTxn currently behaves the same as GenerateManifest.
// The provided transaction is accepted for API compatibility but is NOT
// used to determine object visibility — this always uses the latest
// committed snapshot. MVCC-bound visibility is not yet implemented.
func GenerateManifestForTxn(
	table *catalog.TableEntry,
	txn *txnbase.TxnMVCCNode,
	dataDir string,
) ([]byte, error) {
	// TODO: implement MVCC-bound variant using txn snapshot
	// For now, delegate to the snapshot-less version.
	_ = txn
	return GenerateManifest(table, dataDir)
}
