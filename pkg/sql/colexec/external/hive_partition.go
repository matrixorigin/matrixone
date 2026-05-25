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

package external

import (
	"context"
	"fmt"
	"iter"
	"path"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__"

	maxPartitionCount = 50000
	// warnPartitionCount must be < maxListCalls-1 to be reachable.
	// For single-level: N partitions = N+1 list calls.
	// Requirements specify warn at 10000, but that's unreachable with maxListCalls=10000.
	// Use 5000 as practical threshold for P0.
	warnPartitionCount = 5000
	maxListCalls       = 10000
)

// PartitionFileEntry represents a discovered file within a Hive partition structure.
type PartitionFileEntry struct {
	FilePath string
	FileSize int64
}

// PartitionPredicate represents a filter hint for partition pruning.
type PartitionPredicate struct {
	ColName string
	Op      PartitionOp
	Values  []string
}

type PartitionOp int

const (
	PartOpEq PartitionOp = iota
	PartOpIn
	PartOpBetween
)

// PartitionDiscoveryResult holds the outcome of Hive partition discovery.
type PartitionDiscoveryResult struct {
	Files          []PartitionFileEntry
	PartitionCount int
	PrunedCount    int
	ListCalls      int
	warnEmitted    bool
}

// HivePartSegment is the parsed result of a single Hive partition directory segment.
type HivePartSegment struct {
	Key   string
	Value string
}

// ListDirFunc abstracts directory listing for testability and S3/local duality.
type ListDirFunc func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error]

// NewListDirFunc creates a ListDirFunc backed by GetForETLWithType.
func NewListDirFunc(param *tree.ExternParam) ListDirFunc {
	if param.ScanType == tree.S3 {
		basePath := normalizeExternalPath(param.Filepath)
		fs, baseReadPath, initErr := plan2.GetForETLWithType(param, basePath)
		return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
			if initErr != nil {
				return func(yield func(*fileservice.DirEntry, error) bool) {
					yield(nil, initErr)
				}
			}
			return fs.List(ctx, deriveHiveListReadPath(basePath, baseReadPath, prefix))
		}
	}
	return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		fs, readPath, err := plan2.GetForETLWithType(param, prefix)
		if err != nil {
			return func(yield func(*fileservice.DirEntry, error) bool) {
				yield(nil, err)
			}
		}
		return fs.List(ctx, readPath)
	}
}

func deriveHiveListReadPath(basePath, baseReadPath, prefix string) string {
	prefix = normalizeExternalPath(prefix)
	if prefix == basePath {
		return baseReadPath
	}
	if !strings.HasPrefix(prefix, basePath+"/") {
		return prefix
	}
	rel := strings.TrimPrefix(prefix, basePath+"/")
	if rel == "" {
		return baseReadPath
	}
	if baseReadPath == "" || baseReadPath == "." {
		return rel
	}
	return path.Join(baseReadPath, rel)
}

// normalizeExternalPath ensures consistent path format for prefix matching.
func normalizeExternalPath(p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "etl:") {
		return path.Clean(p)
	}
	return path.Clean("/" + p)
}

// relPartitionPath returns filePath relative to basePath. If filePath is not
// under basePath (degenerate), the normalized filePath is returned unchanged.
// Used for error messages: raw filePath contains machine-local absolute paths
// (e.g. /Users/foo/.../data.parquet) which make BVT .result files non-portable;
// the relative form ("year=abc/data.parquet") is stable across machines.
func relPartitionPath(filePath, basePath string) string {
	f := normalizeExternalPath(filePath)
	b := normalizeExternalPath(basePath)
	if f == b {
		return ""
	}
	if strings.HasPrefix(f, b+"/") {
		return f[len(b)+1:]
	}
	return f
}

// ParseHivePartitionSegment parses a directory segment like "year=2024" into key/value.
// MatrixOne intentionally treats Hive partition segment values as raw path
// segment text. DiscoverHivePartitions rejects '%' before this parser is called,
// so URL-encoded partition directory names are unsupported instead of being
// partially decoded in some call paths.
//
// Returns:
//   - (seg, true, nil): valid key=value segment (value may be empty string)
//   - (_, false, nil): not a key=value format (caller treats as non-partition dir)
func ParseHivePartitionSegment(segment string) (seg HivePartSegment, isHive bool, err error) {
	idx := strings.IndexByte(segment, '=')
	if idx <= 0 {
		return HivePartSegment{}, false, nil
	}
	seg.Key = segment[:idx]
	seg.Value = segment[idx+1:]
	if err := validateHivePartitionSegmentPart("key", seg.Key, false); err != nil {
		return HivePartSegment{}, true, err
	}
	if err := validateHivePartitionSegmentPart("value", seg.Value, true); err != nil {
		return HivePartSegment{}, true, err
	}
	return seg, true, nil
}

func validateHivePartitionSegmentPart(kind, value string, allowEmpty bool) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return moerr.NewInternalErrorNoCtxf("invalid hive partition %s: empty", kind)
	}
	if value == "." || value == ".." {
		return moerr.NewInternalErrorNoCtxf("invalid hive partition %s '%s': path traversal segment is not allowed", kind, value)
	}
	for _, r := range value {
		if r == '/' || r == '\\' {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition %s '%s': path separator is not allowed", kind, value)
		}
		if r == '%' {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition %s '%s': URL-encoded values are not supported", kind, value)
		}
		if r == 0 || r < 0x20 || r == 0x7f {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition %s '%s': control character is not allowed", kind, value)
		}
		if kind == "key" && !isHivePartitionKeyRune(r) {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition key '%s': only letters, digits, and '_' are allowed", value)
		}
	}
	return nil
}

func isHivePartitionKeyRune(r rune) bool {
	return r == '_' ||
		(r >= '0' && r <= '9') ||
		(r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z')
}

// ExtractPartitionValues parses partition key=value segments from a file path
// relative to basePath. Both paths are normalized internally.
func ExtractPartitionValues(filePath, basePath string, partCols []string) (map[string]string, error) {
	filePath = normalizeExternalPath(filePath)
	basePath = normalizeExternalPath(basePath)

	if filePath != basePath && !strings.HasPrefix(filePath, basePath+"/") {
		return nil, moerr.NewInternalErrorNoCtxf(
			"file path '%s' is not under base path '%s'", filePath, basePath)
	}

	rel := strings.TrimPrefix(filePath, basePath)
	rel = strings.TrimPrefix(rel, "/")
	segments := strings.Split(rel, "/")

	values := make(map[string]string, len(partCols))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		seg, isHive, err := ParseHivePartitionSegment(segment)
		if err != nil {
			return nil, err
		}
		if !isHive {
			continue
		}
		values[strings.ToLower(seg.Key)] = seg.Value
	}
	return values, nil
}

// IsHiddenFile returns true for files/dirs starting with '.' or '_'.
func IsHiddenFile(name string) bool {
	return len(name) > 0 && (name[0] == '.' || name[0] == '_')
}

// IsParquetFile returns true for files with .parquet or .snappy.parquet suffix.
func IsParquetFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".parquet")
}

// DiscoverHivePartitions performs recursive list-and-filter partition discovery.
func DiscoverHivePartitions(
	ctx context.Context,
	listDir ListDirFunc,
	basePath string,
	partCols []string,
	colTypes []tree.HivePartColType,
	predicates []PartitionPredicate,
) (*PartitionDiscoveryResult, error) {
	basePath = normalizeExternalPath(basePath)

	if len(colTypes) != len(partCols) {
		colTypes = make([]tree.HivePartColType, len(partCols))
		for i := range colTypes {
			colTypes[i] = tree.HivePartColType{Id: int32(types.T_any)}
		}
	}

	predMap := buildPredicateMap(predicates)

	result := &PartitionDiscoveryResult{}
	err := discoverRecursive(ctx, listDir, basePath, basePath, partCols, colTypes, predMap, 0, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func buildPredicateMap(predicates []PartitionPredicate) map[string]*PartitionPredicate {
	m := make(map[string]*PartitionPredicate, len(predicates))
	for i := range predicates {
		m[predicates[i].ColName] = &predicates[i]
	}
	return m
}

func discoverRecursive(
	ctx context.Context,
	listDir ListDirFunc,
	basePath string,
	prefix string,
	partCols []string,
	colTypes []tree.HivePartColType,
	predMap map[string]*PartitionPredicate,
	level int,
	result *PartitionDiscoveryResult,
) error {
	result.ListCalls++
	if result.ListCalls > maxListCalls {
		return moerr.NewInternalErrorNoCtxf(
			"hive partition discovery exceeded %d List calls; reduce partition depth or add filters", maxListCalls)
	}

	isLastLevel := level == len(partCols)-1
	childPrefixes := make([]string, 0)

	for entry, err := range listDir(ctx, prefix) {
		if err != nil {
			return err
		}

		if IsHiddenFile(entry.Name) {
			continue
		}

		if entry.IsDir {
			if level >= len(partCols) {
				continue
			}

			// URL-encoded partition directories are unsupported. Reject '%' during
			// discovery so values cannot be silently interpreted differently by
			// different code paths.
			if strings.Contains(entry.Name, "%") {
				return moerr.NewInternalErrorNoCtxf(
					"hive partition directory name contains '%%' which is not supported: '%s'", entry.Name)
			}

			seg, isHive, parseErr := ParseHivePartitionSegment(entry.Name)
			if parseErr != nil {
				return parseErr
			}
			if !isHive {
				continue
			}

			if strings.ToLower(seg.Key) != partCols[level] {
				continue
			}

			pred := predMap[partCols[level]]
			if !filterPartitionDir(seg.Value, colTypes[level], pred) {
				result.PrunedCount++
				continue
			}

			result.PartitionCount++
			if result.PartitionCount > maxPartitionCount {
				return moerr.NewInternalErrorNoCtxf(
					"hive partition discovery exceeded %d partitions; consider adding partition filters", maxPartitionCount)
			}
			if !result.warnEmitted && result.PartitionCount > warnPartitionCount {
				result.warnEmitted = true
				logutil.Warnf("hive partition discovery: partition count exceeds %d (current: %d, base: %s); consider adding partition filters",
					warnPartitionCount, result.PartitionCount, basePath)
			}

			childPrefixes = append(childPrefixes, path.Join(prefix, entry.Name))
		}
	}

	// Count all matching partitions at this level before descending. Otherwise
	// a very wide single-level table hits maxListCalls while collecting each
	// leaf before maxPartitionCount can ever fire.
	for _, childPrefix := range childPrefixes {
		if isLastLevel {
			if err := collectFiles(ctx, listDir, childPrefix, result); err != nil {
				return err
			}
		} else {
			if err := discoverRecursive(ctx, listDir, basePath, childPrefix, partCols, colTypes, predMap, level+1, result); err != nil {
				return err
			}
		}
	}
	return nil
}

func collectFiles(
	ctx context.Context,
	listDir ListDirFunc,
	prefix string,
	result *PartitionDiscoveryResult,
) error {
	result.ListCalls++
	if result.ListCalls > maxListCalls {
		return moerr.NewInternalErrorNoCtxf(
			"hive partition discovery exceeded %d List calls; reduce partition depth or add filters", maxListCalls)
	}

	for entry, err := range listDir(ctx, prefix) {
		if err != nil {
			return err
		}
		if entry.IsDir || IsHiddenFile(entry.Name) {
			continue
		}
		if IsParquetFile(entry.Name) {
			result.Files = append(result.Files, PartitionFileEntry{
				FilePath: path.Join(prefix, entry.Name),
				FileSize: entry.Size,
			})
		}
	}
	return nil
}

// filterPartitionDir returns true if the directory should be kept (not pruned).
// Only MatchFalse causes pruning; MatchUnknown is conservative (keeps directory).
func filterPartitionDir(dirValue string, colType tree.HivePartColType, pred *PartitionPredicate) bool {
	if pred == nil {
		return true
	}
	var result MatchResult
	switch pred.Op {
	case PartOpEq, PartOpIn:
		result = matchPartitionValue(dirValue, pred.Values, colType)
	case PartOpBetween:
		result = matchPartitionRange(dirValue, pred.Values, colType)
	default:
		result = MatchUnknown
	}
	return result != MatchFalse
}

// MatchResult is a three-state result for partition value comparison.
type MatchResult int

const (
	MatchTrue    MatchResult = iota // definitely matches
	MatchFalse                      // definitely does not match (safe to prune)
	MatchUnknown                    // cannot determine (must keep directory)
)

// matchPartitionValue compares a partition directory value against predicate values.
// Conservative: returns MatchUnknown whenever precise comparison isn't possible.
func matchPartitionValue(dirValue string, predicateValues []string, colType tree.HivePartColType) MatchResult {
	// SET/ENUM columns stored as numeric types but with Enumvalues must not be
	// pruned numerically — their directory values are member names.
	if !canPruneType(colType) {
		return MatchUnknown
	}
	switch types.T(colType.Id) {
	case types.T_any:
		return MatchUnknown

	case types.T_int8:
		return matchInt(dirValue, predicateValues, 8)
	case types.T_int16:
		return matchInt(dirValue, predicateValues, 16)
	case types.T_int32:
		return matchInt(dirValue, predicateValues, 32)
	case types.T_int64:
		return matchInt(dirValue, predicateValues, 64)

	case types.T_uint8:
		return matchUint(dirValue, predicateValues, 8)
	case types.T_uint16:
		return matchUint(dirValue, predicateValues, 16)
	case types.T_uint32:
		return matchUint(dirValue, predicateValues, 32)
	case types.T_uint64:
		return matchUint(dirValue, predicateValues, 64)

	case types.T_char, types.T_varchar, types.T_text:
		for _, pv := range predicateValues {
			if dirValue == pv {
				return MatchTrue
			}
		}
		return MatchUnknown

	default:
		// bool, float, decimal, date/time, json, uuid, enum, set, bit, etc.
		return MatchUnknown
	}
}

// matchPartitionValueForType checks whether we can safely prune this type.
// SET/ENUM stored as T_uint64/T_uint16 with Enumvalues must NOT be pruned
// numerically — their directory values are member names, not raw integers.
func canPruneType(colType tree.HivePartColType) bool {
	if colType.Enumvalues != "" {
		return false
	}
	switch types.T(colType.Id) {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func matchInt(dirVal string, predVals []string, bitSize int) MatchResult {
	dv, err := strconv.ParseInt(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	for _, pv := range predVals {
		pvi, err := strconv.ParseInt(pv, 10, bitSize)
		if err != nil {
			return MatchUnknown
		}
		if dv == pvi {
			return MatchTrue
		}
	}
	return MatchFalse
}

func matchUint(dirVal string, predVals []string, bitSize int) MatchResult {
	dv, err := strconv.ParseUint(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	for _, pv := range predVals {
		pvi, err := strconv.ParseUint(pv, 10, bitSize)
		if err != nil {
			return MatchUnknown
		}
		if dv == pvi {
			return MatchTrue
		}
	}
	return MatchFalse
}

func matchPartitionRange(dirValue string, bounds []string, colType tree.HivePartColType) MatchResult {
	if len(bounds) != 2 || !canPruneType(colType) {
		return MatchUnknown
	}
	switch types.T(colType.Id) {
	case types.T_int8:
		return matchIntRange(dirValue, bounds[0], bounds[1], 8)
	case types.T_int16:
		return matchIntRange(dirValue, bounds[0], bounds[1], 16)
	case types.T_int32:
		return matchIntRange(dirValue, bounds[0], bounds[1], 32)
	case types.T_int64:
		return matchIntRange(dirValue, bounds[0], bounds[1], 64)
	case types.T_uint8:
		return matchUintRange(dirValue, bounds[0], bounds[1], 8)
	case types.T_uint16:
		return matchUintRange(dirValue, bounds[0], bounds[1], 16)
	case types.T_uint32:
		return matchUintRange(dirValue, bounds[0], bounds[1], 32)
	case types.T_uint64:
		return matchUintRange(dirValue, bounds[0], bounds[1], 64)
	default:
		return MatchUnknown
	}
}

func matchIntRange(dirVal, lowVal, highVal string, bitSize int) MatchResult {
	dv, err := strconv.ParseInt(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	lo, err := strconv.ParseInt(lowVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	hi, err := strconv.ParseInt(highVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	if lo > hi {
		return MatchFalse
	}
	if dv < lo || dv > hi {
		return MatchFalse
	}
	return MatchTrue
}

func matchUintRange(dirVal, lowVal, highVal string, bitSize int) MatchResult {
	dv, err := strconv.ParseUint(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	lo, err := strconv.ParseUint(lowVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	hi, err := strconv.ParseUint(highVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	if lo > hi {
		return MatchFalse
	}
	if dv < lo || dv > hi {
		return MatchFalse
	}
	return MatchTrue
}

// ---------------------------------------------------------------------------
// Filter classification and partition predicate extraction
// ---------------------------------------------------------------------------

// filePathColSet is the set of virtual columns Hive pushes into
// FilterFileList at compile time. Only __mo_filepath qualifies: it is
// appended to every external table's TableDef.Cols (query_builder.go:4902)
// and its value is known before we open any parquet file.
//
// STATEMENT_ACCOUNT ("account") is deliberately excluded. It is not a
// virtual column on Hive/Parquet tables — it is synthesized per-batch by
// makeFilepathBatch (external.go:322) only for CSV external tables' tenant
// filter evaluation. Including it here would misclassify any physical
// column literally named "account" as a filepath filter and evaluate it
// against getAccountCol(path), producing wrong results.
var filePathColSet = map[string]bool{
	catalog.ExternalFilePath: true,
}

// ClassifyFilters splits a filter list into three disjoint groups:
//   - partitionFilters: only reference partition columns (also copied to rowFilters)
//   - filePathFilters: only reference filepath virtual columns (see filePathColSet: __mo_filepath only)
//   - rowFilters: everything else, plus partition filters for double-filtering safety
func ClassifyFilters(
	tableDef *plan.TableDef,
	filters []*plan.Expr,
	partColSet map[string]bool,
) (partitionFilters, filePathFilters, rowFilters []*plan.Expr) {
	for _, f := range filters {
		refs := collectBareColNames(tableDef, f)
		if len(refs) == 0 {
			rowFilters = append(rowFilters, f)
			continue
		}
		if subsetOf(refs, partColSet) {
			partitionFilters = append(partitionFilters, f)
			rowFilters = append(rowFilters, f)
			continue
		}
		if subsetOf(refs, filePathColSet) {
			filePathFilters = append(filePathFilters, f)
			continue
		}
		rowFilters = append(rowFilters, f)
	}
	return
}

// subsetOf returns true if every key in refs exists in allowed.
func subsetOf(refs map[string]bool, allowed map[string]bool) bool {
	if len(refs) == 0 {
		return false
	}
	for name := range refs {
		if !allowed[name] {
			return false
		}
	}
	return true
}

// collectBareColNames extracts the set of bare column names referenced by an expression.
// Uses ColPos to look up names from the pruned TableDef (not col.Name which may contain table prefix).
func collectBareColNames(tableDef *plan.TableDef, expr *plan.Expr) map[string]bool {
	names := map[string]bool{}
	var walk func(e *plan.Expr)
	walk = func(e *plan.Expr) {
		if e == nil {
			return
		}
		switch v := e.Expr.(type) {
		case *plan.Expr_Col:
			colPos := v.Col.ColPos
			if colPos >= 0 && int(colPos) < len(tableDef.Cols) {
				names[strings.ToLower(tableDef.Cols[colPos].Name)] = true
			} else {
				name := v.Col.Name
				if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
					name = name[idx+1:]
				}
				names[strings.ToLower(name)] = true
			}
		case *plan.Expr_F:
			for _, arg := range v.F.Args {
				walk(arg)
			}
		}
	}
	walk(expr)
	return names
}

// ExtractPartitionPredicatesFromExprs converts partition filter expressions
// into PartitionPredicate hints for directory-level pruning.
// Supports col = const (PartOpEq) and col IN (const, ...) (PartOpIn).
// Expressions that cannot be structurally decomposed are silently skipped.
//
// Note: After optimizer constant-folding (rule.ConstantFold applies to all nodes),
// IN lists may be folded from Expr_List to Expr_Vec. Both forms are handled.
func ExtractPartitionPredicatesFromExprs(
	tableDef *plan.TableDef,
	partFilters []*plan.Expr,
	partColSet map[string]bool,
) []PartitionPredicate {
	var preds []PartitionPredicate
	for _, f := range partFilters {
		if pred, ok := tryExtractPredicate(tableDef, f, partColSet); ok {
			preds = append(preds, pred)
		}
	}
	return preds
}

func tryExtractPredicate(tableDef *plan.TableDef, expr *plan.Expr, partColSet map[string]bool) (PartitionPredicate, bool) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		return PartitionPredicate{}, false
	}

	fid, _ := function.DecodeOverloadID(fn.F.Func.GetObj())
	switch fid {
	case function.EQUAL:
		return tryExtractEqual(tableDef, fn.F.Args, partColSet)
	case function.IN:
		return tryExtractIn(tableDef, fn.F.Args, partColSet)
	case function.BETWEEN:
		return tryExtractBetween(tableDef, fn.F.Args, partColSet)
	default:
		return PartitionPredicate{}, false
	}
}

func tryExtractEqual(tableDef *plan.TableDef, args []*plan.Expr, partColSet map[string]bool) (PartitionPredicate, bool) {
	if len(args) != 2 {
		return PartitionPredicate{}, false
	}
	colName, colOk := getPartColName(tableDef, args[0], partColSet)
	litVal, litOk := getLiteralString(args[1])
	if !colOk || !litOk {
		colName, colOk = getPartColName(tableDef, args[1], partColSet)
		litVal, litOk = getLiteralString(args[0])
		if !colOk || !litOk {
			return PartitionPredicate{}, false
		}
	}
	return PartitionPredicate{
		ColName: colName,
		Op:      PartOpEq,
		Values:  []string{litVal},
	}, true
}

func tryExtractIn(tableDef *plan.TableDef, args []*plan.Expr, partColSet map[string]bool) (PartitionPredicate, bool) {
	if len(args) != 2 {
		return PartitionPredicate{}, false
	}
	colName, colOk := getPartColName(tableDef, args[0], partColSet)
	if !colOk {
		return PartitionPredicate{}, false
	}

	// After optimizer constant-folding, IN lists may be Expr_List or Expr_Vec.
	switch v := args[1].Expr.(type) {
	case *plan.Expr_List:
		if v.List == nil {
			return PartitionPredicate{}, false
		}
		values := make([]string, 0, len(v.List.List))
		for _, item := range v.List.List {
			litVal, litOk := getLiteralString(item)
			if !litOk {
				return PartitionPredicate{}, false
			}
			values = append(values, litVal)
		}
		if len(values) == 0 {
			return PartitionPredicate{}, false
		}
		return PartitionPredicate{ColName: colName, Op: PartOpIn, Values: values}, true

	case *plan.Expr_Vec:
		values, ok := extractVecValues(v.Vec, args[0].Typ)
		if !ok || len(values) == 0 {
			return PartitionPredicate{}, false
		}
		return PartitionPredicate{ColName: colName, Op: PartOpIn, Values: values}, true

	default:
		return PartitionPredicate{}, false
	}
}

func tryExtractBetween(tableDef *plan.TableDef, args []*plan.Expr, partColSet map[string]bool) (PartitionPredicate, bool) {
	if len(args) != 3 {
		return PartitionPredicate{}, false
	}
	colName, colOk := getPartColName(tableDef, args[0], partColSet)
	lowVal, lowOk := getLiteralString(args[1])
	highVal, highOk := getLiteralString(args[2])
	if !colOk || !lowOk || !highOk {
		return PartitionPredicate{}, false
	}
	return PartitionPredicate{ColName: colName, Op: PartOpBetween, Values: []string{lowVal, highVal}}, true
}

// extractVecValues decodes a folded LiteralVec into string values for pruning.
func extractVecValues(litVec *plan.LiteralVec, typ plan.Type) (values []string, ok bool) {
	if litVec == nil || litVec.Len <= 0 || len(litVec.Data) == 0 {
		return nil, false
	}
	oid := types.T(typ.Id)
	if !vectorBinaryEnvelopeInBounds(litVec.Data) {
		return nil, false
	}

	vec := vector.NewVec(types.New(oid, typ.Width, typ.Scale))
	// UnmarshalBinary aliases litVec.Data and marks vector buffers non-freeable,
	// so a nil MPool is intentional here.
	defer vec.Free(nil)
	if err := vec.UnmarshalBinary(litVec.Data); err != nil {
		return nil, false
	}
	if vec.GetType().Oid != oid || vec.Length() != int(litVec.Len) {
		return nil, false
	}

	n := vec.Length()
	values = make([]string, 0, n)
	switch oid {
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatInt(int64(col[i]), 10))
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatInt(int64(col[i]), 10))
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatInt(int64(col[i]), 10))
		}
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatInt(col[i], 10))
		}
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatUint(uint64(col[i]), 10))
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatUint(uint64(col[i]), 10))
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatUint(uint64(col[i]), 10))
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		for i := 0; i < n; i++ {
			values = append(values, strconv.FormatUint(col[i], 10))
		}
	case types.T_char, types.T_varchar, types.T_text:
		col := vector.MustFixedColNoTypeCheck[types.Varlena](vec)
		area := vec.GetArea()
		for i := 0; i < n; i++ {
			bs, ok := safeVarlenaBytes(&col[i], area)
			if !ok {
				return nil, false
			}
			values = append(values, string(bs))
		}
	default:
		return nil, false
	}
	return values, true
}

// vectorBinaryEnvelopeInBounds only checks the bounds of Vector.UnmarshalBinary's
// envelope before calling it. It does not validate type semantics; those are
// checked after UnmarshalBinary succeeds.
func vectorBinaryEnvelopeInBounds(data []byte) bool {
	if len(data) == 0 || int(data[0]) != vector.FLAT {
		return false
	}
	pos := 1 + types.TSize
	if len(data) < pos+4 {
		return false
	}
	pos += 4 // vector length

	for i := 0; i < 3; i++ {
		if len(data) < pos+4 {
			return false
		}
		n := types.DecodeUint32(data[pos : pos+4])
		pos += 4
		if uint64(n) > uint64(len(data)-pos) {
			return false
		}
		pos += int(n)
	}
	return len(data) >= pos+1
}

func safeVarlenaBytes(v *types.Varlena, area []byte) ([]byte, bool) {
	if v.IsSmall() {
		return v.ByteSlice(), true
	}
	off, size := v.OffsetLen()
	end := uint64(off) + uint64(size)
	if end > uint64(len(area)) {
		return nil, false
	}
	return area[int(off):int(end)], true
}

// getPartColName returns the bare partition column name from a column expression.
func getPartColName(tableDef *plan.TableDef, expr *plan.Expr, partColSet map[string]bool) (string, bool) {
	col, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		return "", false
	}
	colPos := col.Col.ColPos
	var name string
	if colPos >= 0 && int(colPos) < len(tableDef.Cols) {
		name = strings.ToLower(tableDef.Cols[colPos].Name)
	} else {
		name = col.Col.Name
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			name = name[idx+1:]
		}
		name = strings.ToLower(name)
	}
	if !partColSet[name] {
		return "", false
	}
	return name, true
}

// getLiteralString extracts a string representation from a literal expression.
// Only accepts Expr_Lit (rejects Expr_F such as cast which may change value).
func getLiteralString(expr *plan.Expr) (string, bool) {
	lit, ok := expr.Expr.(*plan.Expr_Lit)
	if !ok || lit.Lit == nil || lit.Lit.Isnull {
		return "", false
	}
	switch v := lit.Lit.Value.(type) {
	case *plan.Literal_Sval:
		return v.Sval, true
	case *plan.Literal_I8Val:
		return strconv.FormatInt(int64(v.I8Val), 10), true
	case *plan.Literal_I16Val:
		return strconv.FormatInt(int64(v.I16Val), 10), true
	case *plan.Literal_I32Val:
		return strconv.FormatInt(int64(v.I32Val), 10), true
	case *plan.Literal_I64Val:
		return strconv.FormatInt(v.I64Val, 10), true
	case *plan.Literal_U8Val:
		return strconv.FormatUint(uint64(v.U8Val), 10), true
	case *plan.Literal_U16Val:
		return strconv.FormatUint(uint64(v.U16Val), 10), true
	case *plan.Literal_U32Val:
		return strconv.FormatUint(uint64(v.U32Val), 10), true
	case *plan.Literal_U64Val:
		return strconv.FormatUint(v.U64Val, 10), true
	case *plan.Literal_Fval:
		return fmt.Sprintf("%g", v.Fval), true
	case *plan.Literal_Dval:
		return fmt.Sprintf("%g", v.Dval), true
	case *plan.Literal_Bval:
		if v.Bval {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}
