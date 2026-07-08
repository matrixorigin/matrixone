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
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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

	maxPartitionPruneExprNodes = 1024
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
	PartOpLt
	PartOpLe
	PartOpGt
	PartOpGe
	PartOpBetween
	PartOpIsNull
	PartOpIsNotNull
)

// PartitionPruneExpr is a conservative, directory-level predicate tree.
// EvalPartial is allowed to return MatchUnknown whenever the currently-known
// partition values are insufficient or SQL semantics cannot be reproduced
// exactly from path strings. Only MatchFalse may prune a directory.
type PartitionPruneExpr interface {
	EvalPartial(values map[string]string, colTypes map[string]tree.HivePartColType) MatchResult
	ReferencedCols() map[string]bool
}

type PartitionAtom struct {
	ColName string
	Op      PartitionOp
	Values  []string
}

type PartitionAnd struct {
	Children []PartitionPruneExpr
}

type PartitionOr struct {
	Children []PartitionPruneExpr
}

func (a *PartitionAtom) EvalPartial(values map[string]string, colTypes map[string]tree.HivePartColType) MatchResult {
	if a == nil {
		return MatchUnknown
	}
	dirValue, ok := values[strings.ToLower(a.ColName)]
	if !ok {
		return MatchUnknown
	}
	colType, ok := colTypes[strings.ToLower(a.ColName)]
	if !ok {
		colType = tree.HivePartColType{Id: int32(types.T_any)}
	}
	switch a.Op {
	case PartOpEq, PartOpIn:
		return matchPartitionValue(dirValue, a.Values, colType)
	case PartOpLt, PartOpLe, PartOpGt, PartOpGe:
		return matchPartitionCompare(dirValue, a.Values, colType, a.Op)
	case PartOpBetween:
		return matchPartitionRange(dirValue, a.Values, colType)
	case PartOpIsNull:
		return matchPartitionNull(dirValue, true)
	case PartOpIsNotNull:
		return matchPartitionNull(dirValue, false)
	default:
		return MatchUnknown
	}
}

func (a *PartitionAtom) ReferencedCols() map[string]bool {
	if a == nil || a.ColName == "" {
		return nil
	}
	return map[string]bool{strings.ToLower(a.ColName): true}
}

func (a *PartitionAnd) EvalPartial(values map[string]string, colTypes map[string]tree.HivePartColType) MatchResult {
	if a == nil || len(a.Children) == 0 {
		return MatchUnknown
	}
	allTrue := true
	for _, child := range a.Children {
		res := child.EvalPartial(values, colTypes)
		if res == MatchFalse {
			return MatchFalse
		}
		if res != MatchTrue {
			allTrue = false
		}
	}
	if allTrue {
		return MatchTrue
	}
	return MatchUnknown
}

func (a *PartitionAnd) ReferencedCols() map[string]bool {
	return mergeReferencedCols(a.Children)
}

func (o *PartitionOr) EvalPartial(values map[string]string, colTypes map[string]tree.HivePartColType) MatchResult {
	if o == nil || len(o.Children) == 0 {
		return MatchUnknown
	}
	allFalse := true
	for _, child := range o.Children {
		res := child.EvalPartial(values, colTypes)
		if res == MatchTrue {
			return MatchTrue
		}
		if res != MatchFalse {
			allFalse = false
		}
	}
	if allFalse {
		return MatchFalse
	}
	return MatchUnknown
}

func (o *PartitionOr) ReferencedCols() map[string]bool {
	return mergeReferencedCols(o.Children)
}

func mergeReferencedCols(children []PartitionPruneExpr) map[string]bool {
	if len(children) == 0 {
		return nil
	}
	refs := make(map[string]bool)
	for _, child := range children {
		for name := range child.ReferencedCols() {
			refs[name] = true
		}
	}
	return refs
}

// PartitionDiscoveryResult holds the outcome of Hive partition discovery.
type PartitionDiscoveryResult struct {
	Files                 []PartitionFileEntry
	PartitionCount        int
	PrunedCount           int
	ListCalls             int
	warnEmitted           bool
	CacheHits             int
	CacheMisses           int
	ListConcurrency       int
	DiscoveryDuration     time.Duration
	InferredPartitionCols []string
	DiscoveredFiles       int
	DiscoveredBytes       int64
	PrunedFiles           int
	PrunedBytes           int64
	DirectPrefixHits      int
	DirectPrefixMisses    int
}

type DiscoverOptions struct {
	MaxPartitions   int
	WarnPartitions  int
	MaxListCalls    int
	ListConcurrency int
	CacheTTL        time.Duration
	CacheKeyPrefix  string
	CacheMaxEntries int
	CacheMaxBytes   int64
	listSemaphore   chan struct{}
	statsMu         *sync.Mutex
	cancel          context.CancelCauseFunc
}

type childPartition struct {
	prefix string
	values map[string]string
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
// Keys stay in the raw path domain and are not URL-decoded. Values are decoded
// exactly once for SQL-visible partition values; callers must continue to use
// raw paths when listing or opening files.
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
	rawValue := segment[idx+1:]
	if err := validateHivePartitionKey(seg.Key); err != nil {
		return HivePartSegment{}, true, err
	}
	if err := validateHivePartitionRawValue(rawValue); err != nil {
		return HivePartSegment{}, true, err
	}
	decodedValue, err := url.PathUnescape(rawValue)
	if err != nil {
		return HivePartSegment{}, true, moerr.NewInternalErrorNoCtxf(
			"invalid URL escape in hive partition value '%s': %v", rawValue, err)
	}
	if err := validateHivePartitionDecodedValue(decodedValue); err != nil {
		return HivePartSegment{}, true, err
	}
	seg.Value = decodedValue
	return seg, true, nil
}

func validateHivePartitionKey(value string) error {
	if value == "" {
		return moerr.NewInternalErrorNoCtx("invalid hive partition key: empty")
	}
	if value == "." || value == ".." {
		return moerr.NewInternalErrorNoCtxf("invalid hive partition key '%s': path traversal segment is not allowed", value)
	}
	for _, r := range value {
		if r == '/' || r == '\\' || r == '%' {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition key '%s': only letters, digits, and '_' are allowed", value)
		}
		if r == 0 || r < 0x20 || r == 0x7f {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition key '%s': control character is not allowed", value)
		}
		if !isHivePartitionKeyRune(r) {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition key '%s': only letters, digits, and '_' are allowed", value)
		}
	}
	return nil
}

func validateHivePartitionRawValue(value string) error {
	for _, r := range value {
		if r == '/' || r == '\\' {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition value '%s': path separator is not allowed", value)
		}
		if r == 0 || r < 0x20 || r == 0x7f {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition value '%s': control character is not allowed", value)
		}
	}
	return nil
}

func validateHivePartitionDecodedValue(value string) error {
	if value == "." || value == ".." {
		return moerr.NewInternalErrorNoCtxf("invalid hive partition value '%s': path traversal segment is not allowed", value)
	}
	for _, r := range value {
		if r == 0 || r < 0x20 || r == 0x7f {
			return moerr.NewInternalErrorNoCtxf("invalid hive partition value '%s': control character is not allowed", value)
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
	return DiscoverHivePartitionsWithPruneExpr(
		ctx,
		listDir,
		basePath,
		partCols,
		colTypes,
		predicateListToPruneExpr(predicates),
		nil,
	)
}

func DiscoverHivePartitionsWithPruneExpr(
	ctx context.Context,
	listDir ListDirFunc,
	basePath string,
	partCols []string,
	colTypes []tree.HivePartColType,
	pruneExpr PartitionPruneExpr,
	opts *DiscoverOptions,
) (*PartitionDiscoveryResult, error) {
	basePath = normalizeExternalPath(basePath)
	options := normalizeDiscoverOptions(opts)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	options.cancel = cancel
	options.statsMu = &sync.Mutex{}
	if options.ListConcurrency > 1 {
		options.listSemaphore = make(chan struct{}, options.ListConcurrency)
	}
	start := time.Now()

	if len(colTypes) != len(partCols) {
		colTypes = make([]tree.HivePartColType, len(partCols))
		for i := range colTypes {
			colTypes[i] = tree.HivePartColType{Id: int32(types.T_any)}
		}
	}

	result := &PartitionDiscoveryResult{ListConcurrency: options.ListConcurrency}
	err := discoverRecursive(ctx, listDir, basePath, basePath, partCols, colTypes,
		toPartitionColTypeMap(partCols, colTypes), pruneExpr, 0, map[string]string{}, result, options)
	result.DiscoveryDuration = time.Since(start)
	if err != nil {
		return nil, err
	}
	sort.Slice(result.Files, func(i, j int) bool {
		return result.Files[i].FilePath < result.Files[j].FilePath
	})
	result.DiscoveredFiles = len(result.Files)
	for _, file := range result.Files {
		result.DiscoveredBytes += file.FileSize
	}
	result.PrunedFiles = result.DiscoveredFiles
	result.PrunedBytes = result.DiscoveredBytes
	return result, nil
}

func normalizeDiscoverOptions(opts *DiscoverOptions) DiscoverOptions {
	options := DiscoverOptions{
		MaxPartitions:   maxPartitionCount,
		WarnPartitions:  warnPartitionCount,
		MaxListCalls:    maxListCalls,
		ListConcurrency: 1,
	}
	if opts == nil {
		return options
	}
	if opts.MaxPartitions > 0 {
		options.MaxPartitions = opts.MaxPartitions
	}
	if opts.WarnPartitions > 0 {
		options.WarnPartitions = opts.WarnPartitions
	}
	if opts.MaxListCalls > 0 {
		options.MaxListCalls = opts.MaxListCalls
	}
	if opts.ListConcurrency > 0 {
		options.ListConcurrency = opts.ListConcurrency
	}
	options.CacheTTL = opts.CacheTTL
	options.CacheKeyPrefix = opts.CacheKeyPrefix
	options.CacheMaxEntries = opts.CacheMaxEntries
	options.CacheMaxBytes = opts.CacheMaxBytes
	return options
}

func addDiscoveryListCall(result *PartitionDiscoveryResult, options DiscoverOptions) error {
	maxCalls := options.MaxListCalls
	if maxCalls <= 0 {
		maxCalls = maxListCalls
	}
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.ListCalls++
	if result.ListCalls > maxCalls {
		err := moerr.NewInternalErrorNoCtxf(
			"hive partition discovery exceeded %d List calls; reduce partition depth or add filters", maxCalls)
		if options.cancel != nil {
			options.cancel(err)
		}
		return err
	}
	return nil
}

func addDiscoveryPartition(result *PartitionDiscoveryResult, options DiscoverOptions, basePath string) error {
	maxPartitions := options.MaxPartitions
	if maxPartitions <= 0 {
		maxPartitions = maxPartitionCount
	}
	warnPartitions := options.WarnPartitions
	if warnPartitions <= 0 {
		warnPartitions = warnPartitionCount
	}
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.PartitionCount++
	if result.PartitionCount > maxPartitions {
		err := moerr.NewInternalErrorNoCtxf(
			"hive partition discovery exceeded %d partitions; consider adding partition filters", maxPartitions)
		if options.cancel != nil {
			options.cancel(err)
		}
		return err
	}
	if !result.warnEmitted && result.PartitionCount > warnPartitions {
		result.warnEmitted = true
		logutil.Warnf("hive partition discovery: partition count exceeds %d (current: %d, base: %s); consider adding partition filters",
			warnPartitions, result.PartitionCount, basePath)
	}
	return nil
}

func addDiscoveryPruned(result *PartitionDiscoveryResult, options DiscoverOptions) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.PrunedCount++
}

func addDiscoveryFile(result *PartitionDiscoveryResult, options DiscoverOptions, file PartitionFileEntry) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.Files = append(result.Files, file)
}

func toPartitionColTypeMap(partCols []string, colTypes []tree.HivePartColType) map[string]tree.HivePartColType {
	m := make(map[string]tree.HivePartColType, len(partCols))
	for i, col := range partCols {
		ct := tree.HivePartColType{Id: int32(types.T_any)}
		if i < len(colTypes) {
			ct = colTypes[i]
		}
		m[strings.ToLower(col)] = ct
	}
	return m
}

func predicateListToPruneExpr(predicates []PartitionPredicate) PartitionPruneExpr {
	if len(predicates) == 0 {
		return nil
	}
	children := make([]PartitionPruneExpr, 0, len(predicates))
	for _, pred := range predicates {
		pred.ColName = strings.ToLower(pred.ColName)
		children = append(children, &PartitionAtom{
			ColName: pred.ColName,
			Op:      pred.Op,
			Values:  append([]string(nil), pred.Values...),
		})
	}
	if len(children) == 1 {
		return children[0]
	}
	return &PartitionAnd{Children: children}
}

func discoverRecursive(
	ctx context.Context,
	listDir ListDirFunc,
	basePath string,
	prefix string,
	partCols []string,
	colTypes []tree.HivePartColType,
	colTypeMap map[string]tree.HivePartColType,
	pruneExpr PartitionPruneExpr,
	level int,
	values map[string]string,
	result *PartitionDiscoveryResult,
	options DiscoverOptions,
) error {
	isLastLevel := level == len(partCols)-1
	childPrefixes := make([]childPartition, 0)

	entries, err := listHivePartitionDir(ctx, listDir, prefix, options, result)
	if err != nil {
		return err
	}
	for i := range entries {
		entry := entries[i]

		if IsHiddenFile(entry.Name) {
			continue
		}

		if entry.IsDir {
			if level >= len(partCols) {
				continue
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

			nextValues := copyPartitionValues(values)
			nextValues[partCols[level]] = seg.Value
			if pruneExpr != nil && pruneExpr.EvalPartial(nextValues, colTypeMap) == MatchFalse {
				addDiscoveryPruned(result, options)
				continue
			}

			if err := addDiscoveryPartition(result, options, basePath); err != nil {
				return err
			}

			childPrefixes = append(childPrefixes, childPartition{
				prefix: path.Join(prefix, entry.Name),
				values: nextValues,
			})
		}
	}

	sort.Slice(childPrefixes, func(i, j int) bool {
		return childPrefixes[i].prefix < childPrefixes[j].prefix
	})

	// Count all matching partitions at this level before descending. Otherwise
	// a very wide single-level table hits maxListCalls while collecting each
	// leaf before maxPartitionCount can ever fire.
	return discoverChildPartitions(ctx, listDir, basePath, partCols, colTypes, colTypeMap, pruneExpr,
		level, isLastLevel, childPrefixes, result, options)
}

func copyPartitionValues(values map[string]string) map[string]string {
	next := make(map[string]string, len(values)+1)
	for k, v := range values {
		next[k] = v
	}
	return next
}

func discoverChildPartitions(
	ctx context.Context,
	listDir ListDirFunc,
	basePath string,
	partCols []string,
	colTypes []tree.HivePartColType,
	colTypeMap map[string]tree.HivePartColType,
	pruneExpr PartitionPruneExpr,
	level int,
	isLastLevel bool,
	childPrefixes []childPartition,
	result *PartitionDiscoveryResult,
	options DiscoverOptions,
) error {
	process := func(childPrefix childPartition) error {
		if isLastLevel {
			return collectFiles(ctx, listDir, childPrefix.prefix, result, options)
		}
		return discoverRecursive(ctx, listDir, basePath, childPrefix.prefix, partCols, colTypes,
			colTypeMap, pruneExpr, level+1, childPrefix.values, result, options)
	}

	if options.ListConcurrency <= 1 || len(childPrefixes) <= 1 {
		for _, childPrefix := range childPrefixes {
			if err := process(childPrefix); err != nil {
				return err
			}
		}
		return nil
	}

	jobs := make(chan childPartition)
	errCh := make(chan error, 1)
	var once sync.Once
	var wg sync.WaitGroup
	workerCount := options.ListConcurrency
	if workerCount > len(childPrefixes) {
		workerCount = len(childPrefixes)
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for childPrefix := range jobs {
				if err := context.Cause(ctx); err != nil {
					once.Do(func() { errCh <- err })
					continue
				}
				if err := process(childPrefix); err != nil {
					if options.cancel != nil {
						options.cancel(err)
					}
					once.Do(func() { errCh <- err })
				}
			}
		}()
	}
sendLoop:
	for _, childPrefix := range childPrefixes {
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- childPrefix:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return context.Cause(ctx)
	}
}

func collectFiles(
	ctx context.Context,
	listDir ListDirFunc,
	prefix string,
	result *PartitionDiscoveryResult,
	options DiscoverOptions,
) error {
	entries, err := listHivePartitionDir(ctx, listDir, prefix, options, result)
	if err != nil {
		return err
	}
	for i := range entries {
		entry := entries[i]
		if entry.IsDir || IsHiddenFile(entry.Name) {
			continue
		}
		if IsParquetFile(entry.Name) {
			addDiscoveryFile(result, options, PartitionFileEntry{
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
	case PartOpLt, PartOpLe, PartOpGt, PartOpGe:
		result = matchPartitionCompare(dirValue, pred.Values, colType, pred.Op)
	case PartOpBetween:
		result = matchPartitionRange(dirValue, pred.Values, colType)
	case PartOpIsNull:
		result = matchPartitionNull(dirValue, true)
	case PartOpIsNotNull:
		result = matchPartitionNull(dirValue, false)
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

func matchPartitionCompare(dirValue string, values []string, colType tree.HivePartColType, op PartitionOp) MatchResult {
	if len(values) != 1 || !canPruneType(colType) {
		return MatchUnknown
	}
	switch types.T(colType.Id) {
	case types.T_int8:
		return matchIntCompare(dirValue, values[0], 8, op)
	case types.T_int16:
		return matchIntCompare(dirValue, values[0], 16, op)
	case types.T_int32:
		return matchIntCompare(dirValue, values[0], 32, op)
	case types.T_int64:
		return matchIntCompare(dirValue, values[0], 64, op)
	case types.T_uint8:
		return matchUintCompare(dirValue, values[0], 8, op)
	case types.T_uint16:
		return matchUintCompare(dirValue, values[0], 16, op)
	case types.T_uint32:
		return matchUintCompare(dirValue, values[0], 32, op)
	case types.T_uint64:
		return matchUintCompare(dirValue, values[0], 64, op)
	default:
		return MatchUnknown
	}
}

func matchIntCompare(dirVal, predVal string, bitSize int, op PartitionOp) MatchResult {
	dv, err := strconv.ParseInt(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	pv, err := strconv.ParseInt(predVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	if compareSigned(dv, pv, op) {
		return MatchTrue
	}
	return MatchFalse
}

func matchUintCompare(dirVal, predVal string, bitSize int, op PartitionOp) MatchResult {
	dv, err := strconv.ParseUint(dirVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	pv, err := strconv.ParseUint(predVal, 10, bitSize)
	if err != nil {
		return MatchUnknown
	}
	if compareUnsigned(dv, pv, op) {
		return MatchTrue
	}
	return MatchFalse
}

func compareSigned(left, right int64, op PartitionOp) bool {
	switch op {
	case PartOpLt:
		return left < right
	case PartOpLe:
		return left <= right
	case PartOpGt:
		return left > right
	case PartOpGe:
		return left >= right
	default:
		return false
	}
}

func compareUnsigned(left, right uint64, op PartitionOp) bool {
	switch op {
	case PartOpLt:
		return left < right
	case PartOpLe:
		return left <= right
	case PartOpGt:
		return left > right
	case PartOpGe:
		return left >= right
	default:
		return false
	}
}

func matchPartitionNull(dirValue string, isNull bool) MatchResult {
	isDefault := dirValue == HiveDefaultPartition
	if isNull == isDefault {
		return MatchTrue
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
		if stagedPart, stagedFilePath, ok := classifyAndFilterForStaging(tableDef, f, partColSet); ok {
			partitionFilters = append(partitionFilters, stagedPart...)
			filePathFilters = append(filePathFilters, stagedFilePath...)
			rowFilters = append(rowFilters, f)
			continue
		}
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

func classifyAndFilterForStaging(
	tableDef *plan.TableDef,
	expr *plan.Expr,
	partColSet map[string]bool,
) (partitionFilters, filePathFilters []*plan.Expr, ok bool) {
	fn, isFn := expr.Expr.(*plan.Expr_F)
	if !isFn {
		return nil, nil, false
	}
	fid, _ := function.DecodeOverloadID(fn.F.Func.GetObj())
	if fid != function.AND {
		return nil, nil, false
	}
	for _, arg := range fn.F.Args {
		refs := collectBareColNames(tableDef, arg)
		if len(refs) == 0 {
			continue
		}
		if subsetOf(refs, partColSet) {
			partitionFilters = append(partitionFilters, arg)
			continue
		}
		if subsetOf(refs, filePathColSet) {
			filePathFilters = append(filePathFilters, arg)
			continue
		}
	}
	return partitionFilters, filePathFilters, len(partitionFilters) > 0 || len(filePathFilters) > 0
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

func ExtractPartitionPruneExprFromExprs(
	tableDef *plan.TableDef,
	partFilters []*plan.Expr,
	partColSet map[string]bool,
) PartitionPruneExpr {
	if len(partFilters) == 0 {
		return nil
	}
	count := 0
	children := make([]PartitionPruneExpr, 0, len(partFilters))
	for _, f := range partFilters {
		expr, ok, exceeded := tryExtractPruneExpr(tableDef, f, partColSet, &count)
		if exceeded {
			return nil
		}
		if ok {
			children = append(children, expr)
		}
	}
	if len(children) == 0 {
		return nil
	}
	if len(children) == 1 {
		return children[0]
	}
	return &PartitionAnd{Children: children}
}

func tryExtractPruneExpr(
	tableDef *plan.TableDef,
	expr *plan.Expr,
	partColSet map[string]bool,
	count *int,
) (PartitionPruneExpr, bool, bool) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		return nil, false, false
	}

	fid, _ := function.DecodeOverloadID(fn.F.Func.GetObj())
	switch fid {
	case function.AND:
		children := make([]PartitionPruneExpr, 0, len(fn.F.Args))
		for _, arg := range fn.F.Args {
			child, ok, exceeded := tryExtractPruneExpr(tableDef, arg, partColSet, count)
			if exceeded {
				return nil, false, true
			}
			if ok {
				children = append(children, child)
			}
		}
		if len(children) == 0 {
			return nil, false, false
		}
		if len(children) == 1 {
			return children[0], true, false
		}
		if !bumpPartitionPruneNode(count) {
			return nil, false, true
		}
		return &PartitionAnd{Children: children}, true, false

	case function.OR:
		children := make([]PartitionPruneExpr, 0, len(fn.F.Args))
		for _, arg := range fn.F.Args {
			child, ok, exceeded := tryExtractPruneExpr(tableDef, arg, partColSet, count)
			if exceeded {
				return nil, false, true
			}
			if !ok {
				return nil, false, false
			}
			children = append(children, child)
		}
		if len(children) == 0 {
			return nil, false, false
		}
		if len(children) == 1 {
			return children[0], true, false
		}
		if !bumpPartitionPruneNode(count) {
			return nil, false, true
		}
		return &PartitionOr{Children: children}, true, false

	case function.EQUAL, function.IN, function.BETWEEN,
		function.GREAT_THAN, function.GREAT_EQUAL, function.LESS_THAN, function.LESS_EQUAL,
		function.ISNULL, function.ISNOTNULL:
		pred, ok := tryExtractPredicateExtended(tableDef, expr, partColSet)
		if !ok {
			return nil, false, false
		}
		if !bumpPartitionPruneNode(count) {
			return nil, false, true
		}
		return &PartitionAtom{
			ColName: pred.ColName,
			Op:      pred.Op,
			Values:  pred.Values,
		}, true, false

	default:
		return nil, false, false
	}
}

func bumpPartitionPruneNode(count *int) bool {
	(*count)++
	return *count <= maxPartitionPruneExprNodes
}

func tryExtractPredicateExtended(tableDef *plan.TableDef, expr *plan.Expr, partColSet map[string]bool) (PartitionPredicate, bool) {
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
	case function.GREAT_THAN, function.GREAT_EQUAL, function.LESS_THAN, function.LESS_EQUAL:
		return tryExtractCompare(tableDef, fn.F.Args, partColSet, fid)
	case function.ISNULL:
		return tryExtractNullCheck(tableDef, fn.F.Args, partColSet, PartOpIsNull)
	case function.ISNOTNULL:
		return tryExtractNullCheck(tableDef, fn.F.Args, partColSet, PartOpIsNotNull)
	default:
		return PartitionPredicate{}, false
	}
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

func tryExtractCompare(tableDef *plan.TableDef, args []*plan.Expr, partColSet map[string]bool, fid int32) (PartitionPredicate, bool) {
	if len(args) != 2 {
		return PartitionPredicate{}, false
	}
	colName, colOk := getPartColName(tableDef, args[0], partColSet)
	litVal, litOk := getLiteralString(args[1])
	op := compareFunctionToPartitionOp(fid)
	if !colOk || !litOk {
		colName, colOk = getPartColName(tableDef, args[1], partColSet)
		litVal, litOk = getLiteralString(args[0])
		op = reversePartitionCompareOp(op)
		if !colOk || !litOk {
			return PartitionPredicate{}, false
		}
	}
	return PartitionPredicate{ColName: colName, Op: op, Values: []string{litVal}}, true
}

func compareFunctionToPartitionOp(fid int32) PartitionOp {
	switch fid {
	case function.GREAT_THAN:
		return PartOpGt
	case function.GREAT_EQUAL:
		return PartOpGe
	case function.LESS_THAN:
		return PartOpLt
	case function.LESS_EQUAL:
		return PartOpLe
	default:
		return PartOpEq
	}
}

func reversePartitionCompareOp(op PartitionOp) PartitionOp {
	switch op {
	case PartOpLt:
		return PartOpGt
	case PartOpLe:
		return PartOpGe
	case PartOpGt:
		return PartOpLt
	case PartOpGe:
		return PartOpLe
	default:
		return op
	}
}

func tryExtractNullCheck(tableDef *plan.TableDef, args []*plan.Expr, partColSet map[string]bool, op PartitionOp) (PartitionPredicate, bool) {
	if len(args) != 1 {
		return PartitionPredicate{}, false
	}
	colName, colOk := getPartColName(tableDef, args[0], partColSet)
	if !colOk {
		return PartitionPredicate{}, false
	}
	return PartitionPredicate{ColName: colName, Op: op}, true
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
	if fn, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function.DecodeOverloadID(fn.F.Func.GetObj())
		if fid == function.UNARY_MINUS && len(fn.F.Args) == 1 {
			return getNegatedLiteralString(fn.F.Args[0])
		}
		return "", false
	}
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

func getNegatedLiteralString(expr *plan.Expr) (string, bool) {
	lit, ok := expr.Expr.(*plan.Expr_Lit)
	if !ok || lit.Lit == nil || lit.Lit.Isnull {
		return "", false
	}
	switch v := lit.Lit.Value.(type) {
	case *plan.Literal_I8Val:
		return strconv.FormatInt(-int64(v.I8Val), 10), true
	case *plan.Literal_I16Val:
		return strconv.FormatInt(-int64(v.I16Val), 10), true
	case *plan.Literal_I32Val:
		return strconv.FormatInt(-int64(v.I32Val), 10), true
	case *plan.Literal_I64Val:
		return strconv.FormatInt(-v.I64Val, 10), true
	case *plan.Literal_U8Val:
		if v.U8Val == 0 {
			return "0", true
		}
		return "-" + strconv.FormatUint(uint64(v.U8Val), 10), true
	case *plan.Literal_U16Val:
		if v.U16Val == 0 {
			return "0", true
		}
		return "-" + strconv.FormatUint(uint64(v.U16Val), 10), true
	case *plan.Literal_U32Val:
		if v.U32Val == 0 {
			return "0", true
		}
		return "-" + strconv.FormatUint(uint64(v.U32Val), 10), true
	case *plan.Literal_U64Val:
		if v.U64Val == 0 {
			return "0", true
		}
		return "-" + strconv.FormatUint(v.U64Val, 10), true
	case *plan.Literal_Fval:
		return fmt.Sprintf("%g", -v.Fval), true
	case *plan.Literal_Dval:
		return fmt.Sprintf("%g", -v.Dval), true
	default:
		return "", false
	}
}
