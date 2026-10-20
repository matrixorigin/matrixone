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

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const formatVersion = 1
const captureMemory = int64(64 << 20)
const captureDisk = int64(4 << 30)

type FileID struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

func identify(path string) (FileID, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return FileID{}, err
	}
	if !stat.Mode().IsRegular() {
		return FileID{}, errors.New("artifact must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return FileID{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return FileID{Path: path, SHA256: hex.EncodeToString(h.Sum(nil)), Bytes: n}, err
}

func (f FileID) verify(root string) error {
	path, err := within(root, f.Path)
	if err != nil {
		return err
	}
	got, err := identify(path)
	if err != nil {
		return err
	}
	if got.SHA256 != f.SHA256 || got.Bytes != f.Bytes {
		return fmt.Errorf("artifact changed: %s", f.Path)
	}
	return nil
}

func within(root, name string) (string, error) {
	if filepath.IsAbs(name) || name == "." || !filepath.IsLocal(name) {
		return "", fmt.Errorf("artifact path must be local: %q", name)
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	path := filepath.Join(base, name)
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, resolved)
	if err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("artifact escapes owned directory: %s", name)
	}
	return resolved, nil
}

func readJSON[T any](path string) (T, error) {
	var value T
	stat, err := os.Stat(path)
	if err != nil {
		return value, err
	}
	if !stat.Mode().IsRegular() || stat.Size() > 8<<20 {
		return value, errors.New("JSON artifact must be a regular file <= 8 MiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return value, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 8<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return value, fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("%s: trailing or oversized JSON", path)
	}
	return value, nil
}

// publishJSON never replaces an existing receipt or manifest.
func publishJSON(path string, value any) (retErr error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, f.Close())
		if retErr != nil {
			_ = os.Remove(path)
		}
	}()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

type Column struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"` // int, decimal, date, text, approximate
	SQLType   string `json:"sql_type,omitempty"`
	Char      bool   `json:"char_padding,omitempty"`
	Reference string `json:"reference_rule,omitempty"`
}

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Key     []int    `json:"primary_key"`
}

func canonicalTables() []Table {
	// Domains and composite keys are independent of the regression-test DDL.
	c := func(name, typ string) Column {
		kind := "text"
		if strings.HasPrefix(typ, "DECIMAL") {
			kind = "decimal"
		} else if typ == "INTEGER" || typ == "BIGINT" {
			kind = "int"
		} else if typ == "DATE" {
			kind = "date"
		}
		return Column{Name: name, Kind: kind, SQLType: typ, Char: strings.HasPrefix(typ, "CHAR(")}
	}
	cols := func(spec string) []Column {
		fields := strings.Fields(spec)
		out := make([]Column, 0, len(fields))
		for _, pair := range fields {
			p := strings.SplitN(pair, ":", 2)
			out = append(out, c(p[0], p[1]))
		}
		return out
	}
	return []Table{
		{"region", cols("r_regionkey:INTEGER r_name:CHAR(25) r_comment:VARCHAR(152)"), []int{0}},
		{"nation", cols("n_nationkey:INTEGER n_name:CHAR(25) n_regionkey:INTEGER n_comment:VARCHAR(152)"), []int{0}},
		{"part", cols("p_partkey:INTEGER p_name:VARCHAR(55) p_mfgr:CHAR(25) p_brand:CHAR(10) p_type:VARCHAR(25) p_size:INTEGER p_container:CHAR(10) p_retailprice:DECIMAL(15,2) p_comment:VARCHAR(23)"), []int{0}},
		{"supplier", cols("s_suppkey:INTEGER s_name:CHAR(25) s_address:VARCHAR(40) s_nationkey:INTEGER s_phone:CHAR(15) s_acctbal:DECIMAL(15,2) s_comment:VARCHAR(101)"), []int{0}},
		{"partsupp", cols("ps_partkey:INTEGER ps_suppkey:INTEGER ps_availqty:INTEGER ps_supplycost:DECIMAL(15,2) ps_comment:VARCHAR(199)"), []int{0, 1}},
		{"customer", cols("c_custkey:INTEGER c_name:VARCHAR(25) c_address:VARCHAR(40) c_nationkey:INTEGER c_phone:CHAR(15) c_acctbal:DECIMAL(15,2) c_mktsegment:CHAR(10) c_comment:VARCHAR(117)"), []int{0}},
		{"orders", cols("o_orderkey:BIGINT o_custkey:INTEGER o_orderstatus:CHAR(1) o_totalprice:DECIMAL(15,2) o_orderdate:DATE o_orderpriority:CHAR(15) o_clerk:CHAR(15) o_shippriority:INTEGER o_comment:VARCHAR(79)"), []int{0}},
		{"lineitem", cols("l_orderkey:BIGINT l_partkey:INTEGER l_suppkey:INTEGER l_linenumber:INTEGER l_quantity:DECIMAL(15,2) l_extendedprice:DECIMAL(15,2) l_discount:DECIMAL(15,2) l_tax:DECIMAL(15,2) l_returnflag:CHAR(1) l_linestatus:CHAR(1) l_shipdate:DATE l_commitdate:DATE l_receiptdate:DATE l_shipinstruct:CHAR(25) l_shipmode:CHAR(10) l_comment:VARCHAR(44)"), []int{0, 3}},
	}
}

func (t Table) ddl() string {
	parts := make([]string, 0, len(t.Columns)+1)
	for _, c := range t.Columns {
		parts = append(parts, c.Name+" "+c.SQLType+" NOT NULL")
	}
	keys := make([]string, len(t.Key))
	for i, k := range t.Key {
		keys[i] = t.Columns[k].Name
	}
	parts = append(parts, "PRIMARY KEY ("+strings.Join(keys, ",")+")")
	return "CREATE TABLE " + t.Name + " (" + strings.Join(parts, ",") + ")"
}

type OrderKey struct {
	Column int  `json:"column"`
	Desc   bool `json:"descending,omitempty"`
}
type Query struct {
	Number          int        `json:"number"`
	Statements      []string   `json:"statements"`
	ResultStatement int        `json:"result_statement"`
	Columns         []Column   `json:"columns"`
	Order           []OrderKey `json:"order"`
	Limit           int        `json:"limit,omitempty"`
	Cleanup         string     `json:"cleanup,omitempty"`
	Source          FileID     `json:"source"`
}

func queryContract(n int) Query {
	q := Query{Number: n}
	// QGEN's qualification projection and ordering. Non-ratio arithmetic stays exact.
	specs := []string{"", "text text decimal decimal decimal decimal approximate approximate approximate int", "decimal text text int text text text text", "int decimal date int", "text int", "text decimal", "decimal", "text text int decimal", "int approximate", "text int decimal", "int text decimal decimal text text text text", "int decimal", "text int int", "int int", "approximate", "int text text text decimal", "text text int int", "approximate", "text int int date decimal decimal", "decimal", "text text", "text int", "text int decimal"}
	for i, k := range strings.Fields(specs[n]) {
		col := Column{Name: fmt.Sprintf("column_%d", i+1), Kind: k, Reference: "exact"}
		if k == "approximate" {
			col.Reference = "ratio"
		}
		q.Columns = append(q.Columns, col)
	}
	// Approximate projections contain one final AVG/division of exact inputs.
	// Derive each fixed-point error from its qualified physical result scale;
	// never infer precision from observed discrepancies or decimal text length.
	orders := map[int][]OrderKey{
		1: {{0, false}, {1, false}}, 2: {{0, true}, {1, false}, {2, false}, {3, false}}, 3: {{1, true}, {2, false}}, 4: {{0, false}}, 5: {{1, true}}, 7: {{0, false}, {1, false}, {2, false}}, 8: {{0, false}}, 9: {{0, false}, {1, true}}, 10: {{2, true}}, 11: {{1, true}}, 12: {{0, false}}, 13: {{1, true}, {0, true}}, 15: {{0, false}}, 16: {{3, true}, {0, false}, {1, false}, {2, false}}, 18: {{4, true}, {3, false}}, 20: {{0, false}}, 21: {{1, true}, {0, false}}, 22: {{0, false}},
	}
	q.Order = orders[n]
	q.Limit = map[int]int{2: 100, 3: 10, 10: 20, 18: 100, 21: 100}[n]
	// Fixed CHAR domains in the selected projection. VARCHAR values retain spaces.
	chars := map[int][]int{1: {0, 1}, 2: {1, 2, 4, 6}, 4: {0}, 5: {0}, 7: {0, 1}, 9: {0}, 10: {4, 6}, 12: {0}, 15: {1, 3}, 16: {0}, 20: {0}, 21: {0}}
	for _, i := range chars[n] {
		q.Columns[i].Char = true
	}
	// TPC-H 3.0.1 clause 2.1.3.5: monetary SUM +/-100; ratios/AVG
	// rounded to cents within 1%; quantities/counts and singleton values exact.
	monetary := map[int][]int{1: {3, 4, 5}, 3: {1}, 5: {1}, 6: {0}, 7: {3}, 9: {2}, 10: {2}, 11: {1}, 15: {4}, 19: {0}, 22: {2}}
	for _, i := range monetary[n] {
		q.Columns[i].Reference = "sum_money"
	}
	return q
}

var viewName = regexp.MustCompile(`\brevenue[0-9]+\b`)

func (q Query) rendered(token string) Query {
	q.Statements = append([]string(nil), q.Statements...)
	for i, s := range q.Statements {
		q.Statements[i] = strings.ReplaceAll(s, "{{view}}", "tpch_revenue_"+token)
	}
	q.Cleanup = strings.ReplaceAll(q.Cleanup, "{{view}}", "tpch_revenue_"+token)
	return q
}

type Generator struct {
	SourceRevision string   `json:"source_revision"`
	Version        string   `json:"version"`
	DBGen          FileID   `json:"dbgen"`
	QGen           FileID   `json:"qgen"`
	Distribution   FileID   `json:"distribution"`
	Templates      []FileID `json:"templates"`
}

type Dataset struct {
	Version       int               `json:"version"`
	Specification string            `json:"specification"`
	Scale         string            `json:"scale"`
	Generator     Generator         `json:"generator"`
	Inputs        map[string]FileID `json:"inputs"`
	Tables        []Table           `json:"tables"`
	Queries       []Query           `json:"queries"`
	References    map[string]FileID `json:"sf1_references,omitempty"`
}

func loadDataset(root string, verifyData bool) (Dataset, error) {
	d, err := readJSON[Dataset](filepath.Join(root, "dataset.json"))
	if err != nil {
		return d, err
	}
	if d.Version != formatVersion || d.Specification != "3.0.1" || len(d.Tables) != 8 || len(d.Queries) != 22 || len(d.Inputs) != 8 {
		return d, errors.New("incomplete dataset contract")
	}
	if !reflect.DeepEqual(d.Tables, canonicalTables()) {
		return d, errors.New("dataset schema differs from canonical TPC-H contract")
	}
	scale, err := strconv.ParseFloat(d.Scale, 64)
	if err != nil || math.IsNaN(scale) || math.IsInf(scale, 0) || scale <= 0 || scale > 1000 {
		return d, errors.New("invalid dataset scale")
	}
	seen := map[int]bool{}
	for i, q := range d.Queries {
		if q.Number < 1 || q.Number > 22 || seen[q.Number] || len(q.Columns) == 0 || q.ResultStatement < 0 || q.ResultStatement >= len(q.Statements) {
			return d, errors.New("invalid query contract")
		}
		seen[q.Number] = true
		if q.Source.Bytes <= 0 || q.Source.Bytes > 1<<20 {
			return d, errors.New("query source size is outside its bound")
		}
		if q.Number != i+1 {
			return d, errors.New("query manifest must be in Q1-Q22 order")
		}
		if err := q.Source.verify(root); err != nil {
			return d, err
		}
		raw, err := os.ReadFile(filepath.Join(root, q.Source.Path))
		if err != nil {
			return d, err
		}
		want, err := renderQGen(q.Number, string(raw))
		if err != nil {
			return d, err
		}
		want.Source = q.Source
		if !reflect.DeepEqual(q, want) {
			return d, fmt.Errorf("query Q%d differs from its reviewed source translation", q.Number)
		}
	}
	for _, t := range d.Tables {
		if _, ok := d.Inputs[t.Name]; !ok {
			return d, fmt.Errorf("missing %s input", t.Name)
		}
	}
	if verifyData {
		for _, f := range d.Inputs {
			if err := f.verify(root); err != nil {
				return d, err
			}
		}
	}
	return d, nil
}
