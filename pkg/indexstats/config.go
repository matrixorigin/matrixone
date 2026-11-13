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

// Package indexstats defines the read-only index diagnostics contract.
package indexstats

const FunctionName = "mo_index_stats"

// Config is produced by the planner after resolving the authorized source.
// Hidden table names are never taken directly from SQL function arguments.
type Config struct {
	Algorithm                                  string
	FullTextVersion                            int64
	Columns                                    []string
	Parser                                     string
	Database, Table, Index, Column, PrimaryKey string
	Metadata, Centroids, Entries               string
	OpType, Quantization                       string
	Async                                      bool
	TableID                                    uint64
	Lists                                      int64
	Options                                    map[string]string
	Tables                                     []IndexTable
	KeyKind, Method                            string
	KeyColumns, StorageColumns                 []string
	PrefixLengths                              map[string]int
}

type IndexTable struct {
	Kind string
	Name string
}

// FullTextSummary counts only live documents and terms with positive live DF.
type FullTextSummary struct {
	Documents, Terms, MinDF, MaxDF int64
	MeanDF                         float64
}

type KeySummary struct {
	Entries, Cardinality int64
}
