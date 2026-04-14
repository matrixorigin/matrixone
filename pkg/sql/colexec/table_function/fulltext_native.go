// Copyright 2025 Matrix Origin
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

package table_function

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type nativePreparedScan struct {
	rel         engine.Relation
	pkType      types.T
	indexTable  string
	objects     []nativeObjectSegment
	complete    bool
	tombstones  engine.Tombstoner
	snapshot    types.TS
	fs          fileservice.FileService
	totalDocs   int64
	totalTokens int64
}

type nativeObjectSegment struct {
	key             string
	name            objectio.ObjectName
	segment         *ftnative.Segment
	applyTombstones bool
}

type nativeDocState struct {
	pk              any
	docLen          int32
	ref             ftnative.RowRef
	obj             objectio.ObjectName
	segmentKey      string
	applyTombstones bool
	counts          []uint16
}

type nativeDocSet map[string]struct{}

type nativeDeleteCache struct {
	hasBlock map[string]bool
	deleted  map[string]map[uint32]bool
}

type nativeTailBatchAttrs struct {
	rowIDIdx  int
	pkIdx     int
	partIdxes []int
	partTypes []types.T
}

type nativeTailSegmentBuilder struct {
	name    objectio.ObjectName
	builder *ftnative.Builder
}

func fulltextIndexMatchNative(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	srctbl, tblname string,
) (bool, error) {
	if !nativeQuerySupported(s) {
		return false, nil
	}

	scan, err := prepareNativeScan(proc, srctbl, tblname, u.param)
	if err != nil || scan == nil {
		return false, err
	}
	if u.param.NativeOnly() {
		scan.complete = true
	}
	if scan.complete {
		applyNativeSegmentStats(u, s, scan)
	}

	if s.Mode == int64(tree.FULLTEXT_DEFAULT) || s.Mode == int64(tree.FULLTEXT_NL) {
		return scan.complete, populatePhraseCompat(u, proc, s, scan, s.Pattern)
	}
	if len(s.Pattern) == 1 && s.Pattern[0].Operator == fulltext.PHRASE {
		return scan.complete, populatePhraseCompat(u, proc, s, scan, s.Pattern[0].Children)
	}
	return scan.complete, populateBooleanNative(u, proc, s, scan)
}

func nativeQuerySupported(s *fulltext.SearchAccum) bool {
	switch s.Mode {
	case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
		for _, p := range s.Pattern {
			if p.Operator != fulltext.TEXT {
				return false
			}
		}
		return true
	case int64(tree.FULLTEXT_BOOLEAN):
		for _, p := range s.Pattern {
			if !nativePatternSupported(p) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func nativePatternSupported(p *fulltext.Pattern) bool {
	switch p.Operator {
	case fulltext.TEXT, fulltext.STAR, fulltext.PLUS, fulltext.MINUS,
		fulltext.LESSTHAN, fulltext.GREATERTHAN, fulltext.RANKLESS,
		fulltext.GROUP, fulltext.PHRASE, fulltext.JOIN:
	default:
		return false
	}
	for _, child := range p.Children {
		if !nativePatternSupported(child) {
			return false
		}
	}
	return true
}

func prepareNativeScan(
	proc *process.Process,
	srctbl, tblname string,
	param fulltext.FullTextParserParam,
) (*nativePreparedScan, error) {
	if len(param.Parts) == 0 {
		return nil, nil
	}

	dbName, tableName, err := parseQualifiedTableName(srctbl)
	if err != nil {
		return nil, err
	}
	_, indexTableName, err := parseQualifiedTableName(tblname)
	if err != nil {
		return nil, err
	}

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbName, proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(proc.Ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	tableDef := rel.GetTableDef(proc.Ctx)
	if hasDatalinkPart(tableDef, param.Parts) {
		return nil, nil
	}
	objectFS, err := colexec.GetObjectFSFromProc(proc)
	if err != nil {
		return nil, err
	}

	visibleInfos, err := rel.GetColumMetadataScanInfo(proc.Ctx, param.Parts[0], false)
	if err != nil {
		return nil, err
	}
	visible := make(map[string]struct{}, len(visibleInfos))
	for _, info := range visibleInfos {
		visible[info.ObjectName] = struct{}{}
	}

	stats, err := rel.GetNonAppendableObjectStats(proc.Ctx)
	if err != nil {
		return nil, err
	}

	objects := make([]nativeObjectSegment, 0, len(stats))
	totalDocs := int64(0)
	totalTokens := int64(0)
	incomplete := false
	for i := range stats {
		name := stats[i].ObjectName()
		nameStr := name.String()
		if _, ok := visible[nameStr]; !ok {
			continue
		}
		delete(visible, nameStr)
		seg, exists, err := ftnative.ReadSidecar(proc.Ctx, objectFS, name, indexTableName)
		if err != nil {
			return nil, err
		}
		if !exists {
			incomplete = true
			continue
		}
		objects = append(objects, nativeObjectSegment{
			key:             nameStr,
			name:            name,
			segment:         seg,
			applyTombstones: true,
		})
		totalDocs += seg.DocCount
		totalTokens += seg.TokenSum
	}
	if len(visible) > 0 {
		incomplete = true
	}

	tailObjects, tailDocs, tailTokens, err := buildNativeTailSegments(proc, rel, tableDef, param)
	if err != nil {
		return nil, err
	}
	if len(tailObjects) > 0 {
		objects = append(objects, tailObjects...)
		totalDocs += tailDocs
		totalTokens += tailTokens
	}
	if len(objects) == 0 {
		return nil, nil
	}

	tombstones, err := rel.CollectTombstones(proc.Ctx, 0, engine.Policy_CollectAllTombstones)
	if err != nil {
		return nil, err
	}

	pkColIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("native fulltext scan missing primary key column")
	}
	return &nativePreparedScan{
		rel:         rel,
		pkType:      types.T(tableDef.Cols[pkColIdx].Typ.Id),
		indexTable:  indexTableName,
		objects:     objects,
		complete:    !incomplete,
		tombstones:  tombstones,
		snapshot:    types.TimestampToTS(proc.GetTxnOperator().SnapshotTS()),
		fs:          objectFS,
		totalDocs:   totalDocs,
		totalTokens: totalTokens,
	}, nil
}

func applyNativeSegmentStats(u *fulltextState, s *fulltext.SearchAccum, scan *nativePreparedScan) {
	s.Nrow = scan.totalDocs
	if scan.totalDocs > 0 {
		s.AvgDocLen = float64(scan.totalTokens) / float64(scan.totalDocs)
	} else {
		s.AvgDocLen = 0
	}
	if s.ScoreAlgo == fulltext.ALGO_BM25 && s.AvgDocLen == 0 {
		s.ScoreAlgo = fulltext.ALGO_TFIDF
	}
	u.statsLoaded = true
}

func populatePhraseCompat(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	scan *nativePreparedScan,
	patterns []*fulltext.Pattern,
) error {
	if len(patterns) == 0 {
		return nil
	}
	tokens := make([]ftnative.PhraseToken, 0, len(patterns))
	for _, p := range patterns {
		if p.Operator != fulltext.TEXT {
			return nil
		}
		tokens = append(tokens, ftnative.PhraseToken{
			Word: p.Text,
			Pos:  p.Position,
		})
	}

	cache := newNativeDeleteCache()
	count := int64(0)
	for _, obj := range scan.objects {
		matches := obj.segment.SearchPhrase(tokens)
		for _, match := range matches {
			if deleted, err := isNativeDeleted(proc.Ctx, scan, cache, obj, match.Ref); err != nil {
				return err
			} else if deleted {
				continue
			}
			addr, buf, err := u.mpool.NewItem()
			if err != nil {
				return err
			}
			for i := 0; i < s.Nkeywords; i++ {
				buf[i] = 1
			}
			pk := decodeNativePK(match.Ref.PK, scan.pkType)
			u.agghtab[pk] = addr
			u.docLenMap[pk] = match.DocLen
			markNativeOwned(u, pk)
			count++
		}
	}
	for i := range u.aggcnt {
		u.aggcnt[i] = count
	}
	return nil
}

func populateBooleanNative(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	scan *nativePreparedScan,
) error {
	leafs := make(map[int32]*fulltext.Pattern, s.Nkeywords)
	phrases := make([]*fulltext.Pattern, 0, 4)
	collectNativePatterns(s.Pattern, leafs, &phrases)

	docs := make(map[string]*nativeDocState, 1024)
	leafSets := make(map[int32]nativeDocSet, len(leafs))
	for i := 0; i < s.Nkeywords; i++ {
		leafSets[int32(i)] = make(nativeDocSet)
	}

	for _, obj := range scan.objects {
		for idx, leaf := range leafs {
			postings := nativeLookupLeaf(obj.segment, leaf)
			for _, posting := range postings {
				key := nativeRowKey(obj.key, posting.Ref)
				state := docs[key]
				if state == nil {
					state = &nativeDocState{
						pk:              decodeNativePK(posting.Ref.PK, scan.pkType),
						docLen:          posting.DocLen,
						ref:             posting.Ref,
						obj:             obj.name,
						segmentKey:      obj.key,
						applyTombstones: obj.applyTombstones,
						counts:          make([]uint16, s.Nkeywords),
					}
					docs[key] = state
				}
				tf := uint16(len(posting.Positions))
				if tf == 0 {
					tf = 1
				}
				state.counts[int(idx)] += tf
				leafSets[idx][key] = struct{}{}
			}
		}
	}
	for i := range u.aggcnt {
		u.aggcnt[i] = int64(len(leafSets[int32(i)]))
	}

	phraseSets := make(map[*fulltext.Pattern]nativeDocSet, len(phrases))
	for _, phrase := range phrases {
		phraseSets[phrase] = make(nativeDocSet)
	}
	for _, obj := range scan.objects {
		for _, phrase := range phrases {
			tokens := make([]ftnative.PhraseToken, 0, len(phrase.Children))
			for _, child := range phrase.Children {
				tokens = append(tokens, ftnative.PhraseToken{
					Word: child.Text,
					Pos:  child.Position,
				})
			}
			for _, match := range obj.segment.SearchPhrase(tokens) {
				phraseSets[phrase][nativeRowKey(obj.key, match.Ref)] = struct{}{}
			}
		}
	}

	candidates := nativeCandidateSet(s, leafSets, phraseSets)
	cache := newNativeDeleteCache()
	for key := range candidates {
		state := docs[key]
		if state == nil {
			continue
		}
		if deleted, err := isNativeDeleted(proc.Ctx, scan, cache, nativeObjectSegment{
			key:             state.segmentKey,
			name:            state.obj,
			applyTombstones: state.applyTombstones,
		}, state.ref); err != nil {
			return err
		} else if deleted {
			continue
		}

		addr, buf, err := u.mpool.NewItem()
		if err != nil {
			return err
		}
		for i, cnt := range state.counts {
			if cnt > 255 {
				buf[i] = 255
			} else {
				buf[i] = uint8(cnt)
			}
		}
		u.agghtab[state.pk] = addr
		u.docLenMap[state.pk] = state.docLen
		markNativeOwned(u, state.pk)
	}
	return nil
}

func markNativeOwned(u *fulltextState, pk any) {
	if u.nativeOwned == nil {
		u.nativeOwned = make(map[any]struct{}, 1024)
	}
	u.nativeOwned[pk] = struct{}{}
}

func collectNativePatterns(patterns []*fulltext.Pattern, leafs map[int32]*fulltext.Pattern, phrases *[]*fulltext.Pattern) {
	for _, p := range patterns {
		switch p.Operator {
		case fulltext.TEXT, fulltext.STAR, fulltext.JOIN:
			leafs[p.Index] = p
		case fulltext.PHRASE:
			*phrases = append(*phrases, p)
			collectNativePatterns(p.Children, leafs, phrases)
		default:
			collectNativePatterns(p.Children, leafs, phrases)
		}
	}
}

func nativeLookupLeaf(seg *ftnative.Segment, leaf *fulltext.Pattern) []ftnative.Posting {
	switch leaf.Operator {
	case fulltext.TEXT, fulltext.JOIN:
		return seg.Lookup(leaf.Text)
	case fulltext.STAR:
		prefix := strings.TrimSuffix(leaf.Text, "*")
		postings := make([]ftnative.Posting, 0, 8)
		for term, termPostings := range seg.Terms {
			if strings.HasPrefix(term, prefix) {
				postings = append(postings, termPostings...)
			}
		}
		return postings
	default:
		return nil
	}
}

func nativeCandidateSet(
	s *fulltext.SearchAccum,
	leafSets map[int32]nativeDocSet,
	phraseSets map[*fulltext.Pattern]nativeDocSet,
) nativeDocSet {
	var result nativeDocSet
	hasPlus := s.PatternAnyPlus()
	for _, p := range s.Pattern {
		arg := nativePatternSet(p, leafSets, phraseSets)
		switch p.Operator {
		case fulltext.MINUS:
			if result != nil {
				nativeDifference(result, arg)
			} else {
				result = make(nativeDocSet)
			}
		case fulltext.PLUS, fulltext.JOIN:
			if result == nil {
				result = nativeCloneSet(arg)
			} else {
				result = nativeIntersect(result, arg)
			}
		default:
			if !hasPlus {
				if result == nil {
					result = nativeCloneSet(arg)
				} else {
					nativeUnion(result, arg)
				}
			} else if result == nil {
				result = nativeCloneSet(arg)
			}
		}
	}
	if result == nil {
		return make(nativeDocSet)
	}
	return result
}

func nativePatternSet(
	p *fulltext.Pattern,
	leafSets map[int32]nativeDocSet,
	phraseSets map[*fulltext.Pattern]nativeDocSet,
) nativeDocSet {
	switch p.Operator {
	case fulltext.TEXT, fulltext.STAR, fulltext.JOIN:
		return leafSets[p.Index]
	case fulltext.PHRASE:
		return phraseSets[p]
	case fulltext.PLUS, fulltext.MINUS, fulltext.LESSTHAN, fulltext.GREATERTHAN, fulltext.RANKLESS:
		if len(p.Children) == 0 {
			return make(nativeDocSet)
		}
		return nativePatternSet(p.Children[0], leafSets, phraseSets)
	case fulltext.GROUP:
		ret := make(nativeDocSet)
		for _, child := range p.Children {
			nativeUnion(ret, nativePatternSet(child, leafSets, phraseSets))
		}
		return ret
	default:
		return make(nativeDocSet)
	}
}

func newNativeDeleteCache() *nativeDeleteCache {
	return &nativeDeleteCache{
		hasBlock: make(map[string]bool),
		deleted:  make(map[string]map[uint32]bool),
	}
}

func isNativeDeleted(
	ctx context.Context,
	scan *nativePreparedScan,
	cache *nativeDeleteCache,
	obj nativeObjectSegment,
	ref ftnative.RowRef,
) (bool, error) {
	if !obj.applyTombstones {
		return false, nil
	}
	bid := objectio.NewBlockidWithObjectID(obj.name.ObjectId(), ref.Block)
	blockKey := nativeBlockKey(obj.key, ref.Block)
	has, ok := cache.hasBlock[blockKey]
	if !ok {
		var err error
		has, err = scan.tombstones.HasBlockTombstone(ctx, &bid, scan.fs)
		if err != nil {
			return false, err
		}
		cache.hasBlock[blockKey] = has
	}
	if !has {
		return false, nil
	}

	if cache.deleted[blockKey] == nil {
		cache.deleted[blockKey] = make(map[uint32]bool)
	}
	if deleted, ok := cache.deleted[blockKey][ref.Row]; ok {
		return deleted, nil
	}

	rows := []int64{int64(ref.Row)}
	rows = scan.tombstones.ApplyInMemTombstones(&bid, rows, nil)
	if len(rows) > 0 {
		var err error
		rows, err = scan.tombstones.ApplyPersistedTombstones(
			ctx,
			scan.fs,
			&scan.snapshot,
			&bid,
			rows,
			nil,
		)
		if err != nil {
			return false, err
		}
	}
	deleted := len(rows) == 0
	cache.deleted[blockKey][ref.Row] = deleted
	return deleted, nil
}

func buildNativeTailSegments(
	proc *process.Process,
	rel engine.Relation,
	tableDef *pbplan.TableDef,
	param fulltext.FullTextParserParam,
) ([]nativeObjectSegment, int64, int64, error) {
	readAttrs, colTypes, pkType, err := buildNativeTailReadAttrs(tableDef, param.Parts)
	if err != nil {
		return nil, 0, 0, err
	}
	relData, err := rel.Ranges(proc.Ctx, engine.RangesParam{
		PreAllocBlocks:     2,
		TxnOffset:          0,
		Policy:             engine.Policy_CollectCommittedInmemData | engine.Policy_CollectUncommittedData,
		DontSupportRelData: false,
	})
	if err != nil {
		return nil, 0, 0, err
	}
	readers, err := rel.BuildReaders(
		proc.Ctx,
		proc,
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckAll,
		engine.FilterHint{},
	)
	if err != nil {
		return nil, 0, 0, err
	}

	builders := make(map[string]*nativeTailSegmentBuilder)
	for _, reader := range readers {
		readBatch := batch.NewWithSize(len(readAttrs))
		readBatch.SetAttributes(readAttrs)
		for i := range readAttrs {
			readBatch.Vecs[i] = vector.NewVec(colTypes[i])
		}
		resolved, err := resolveNativeTailBatchAttrs(readBatch, tableDef.Pkey.PkeyColName, param.Parts)
		if err != nil {
			readBatch.Clean(proc.Mp())
			reader.Close()
			return nil, 0, 0, err
		}
		func() {
			defer readBatch.Clean(proc.Mp())
			defer reader.Close()
			for {
				isEnd, readErr := reader.Read(proc.Ctx, readAttrs, nil, proc.Mp(), readBatch)
				if readErr != nil {
					err = readErr
					return
				}
				if isEnd {
					return
				}
				if readBatch.RowCount() == 0 {
					readBatch.CleanOnlyData()
					continue
				}
				readErr = appendNativeTailBatch(
					builders,
					readBatch,
					resolved,
					pkType,
					param,
				)
				readBatch.CleanOnlyData()
				if readErr != nil {
					err = readErr
					return
				}
			}
		}()
		if err != nil {
			return nil, 0, 0, err
		}
	}
	objects, totalDocs, totalTokens := buildNativeTailSegmentsFromBuilders(builders)
	return objects, totalDocs, totalTokens, nil
}

func buildNativeTailReadAttrs(
	tableDef *pbplan.TableDef,
	parts []string,
) ([]string, []types.Type, types.T, error) {
	pkIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column")
	}
	pkType := types.T(tableDef.Cols[pkIdx].Typ.Id)
	readAttrs := make([]string, 0, len(parts)+2)
	colTypes := make([]types.Type, 0, len(parts)+2)
	seen := make(map[string]struct{}, len(parts)+2)
	appendAttr := func(name string, typ types.Type) {
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		readAttrs = append(readAttrs, name)
		colTypes = append(colTypes, typ)
	}
	appendAttr(catalog.Row_ID, types.T_Rowid.ToType())
	appendAttr(
		tableDef.Pkey.PkeyColName,
		types.New(
			types.T(tableDef.Cols[pkIdx].Typ.Id),
			tableDef.Cols[pkIdx].Typ.Width,
			tableDef.Cols[pkIdx].Typ.Scale,
		),
	)
	for _, part := range parts {
		idx, ok := tableDef.Name2ColIndex[part]
		if !ok {
			return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column")
		}
		appendAttr(
			part,
			types.New(
				types.T(tableDef.Cols[idx].Typ.Id),
				tableDef.Cols[idx].Typ.Width,
				tableDef.Cols[idx].Typ.Scale,
			),
		)
	}
	return readAttrs, colTypes, pkType, nil
}

func resolveNativeTailBatchAttrs(
	bat *batch.Batch,
	pkName string,
	parts []string,
) (nativeTailBatchAttrs, error) {
	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}

	rowIDIdx, ok := attrMap[strings.ToLower(catalog.Row_ID)]
	if !ok {
		return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing rowid column in batch")
	}
	pkIdx, ok := attrMap[strings.ToLower(pkName)]
	if !ok {
		return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column in batch")
	}

	partIdxes := make([]int, 0, len(parts))
	partTypes := make([]types.T, 0, len(parts))
	for _, part := range parts {
		colIdx, ok := attrMap[strings.ToLower(part)]
		if !ok {
			return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column in batch")
		}
		partIdxes = append(partIdxes, colIdx)
		partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
	}

	return nativeTailBatchAttrs{
		rowIDIdx:  rowIDIdx,
		pkIdx:     pkIdx,
		partIdxes: partIdxes,
		partTypes: partTypes,
	}, nil
}

func appendNativeTailBatch(
	builders map[string]*nativeTailSegmentBuilder,
	bat *batch.Batch,
	resolved nativeTailBatchAttrs,
	pkType types.T,
	param fulltext.FullTextParserParam,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	for row := 0; row < bat.RowCount(); row++ {
		values, ok := collectNativeTailIndexValues(bat, row, resolved.partIdxes, resolved.partTypes)
		if !ok {
			continue
		}
		rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[resolved.rowIDIdx], row)
		objName := objectio.BuildObjectNameWithObjectID(rowID.BorrowObjectID())
		objKey := objName.String()
		tailBuilder := builders[objKey]
		if tailBuilder == nil {
			tailBuilder = &nativeTailSegmentBuilder{
				name:    objName,
				builder: ftnative.NewBuilder(param, nil),
			}
			builders[objKey] = tailBuilder
		}
		pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[resolved.pkIdx], row, true), pkType)
		if err := tailBuilder.builder.Add(ftnative.Document{
			Block:  rowID.GetBlockOffset(),
			Row:    rowID.GetRowOffset(),
			PK:     pkBytes,
			Values: values,
		}); err != nil {
			return err
		}
	}
	return nil
}

func collectNativeTailIndexValues(
	bat *batch.Batch,
	row int,
	partIdxes []int,
	partTypes []types.T,
) ([]fulltext.IndexValue, bool) {
	values := make([]fulltext.IndexValue, 0, len(partIdxes))
	for i, partIdx := range partIdxes {
		vec := bat.Vecs[partIdx]
		if vec.IsNull(uint64(row)) {
			continue
		}
		values = append(values, fulltext.IndexValue{
			Text: vec.GetStringAt(row),
			Raw:  vec.GetRawBytesAt(row),
			Type: partTypes[i],
		})
	}
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

func buildNativeTailSegmentsFromBuilders(
	builders map[string]*nativeTailSegmentBuilder,
) ([]nativeObjectSegment, int64, int64) {
	if len(builders) == 0 {
		return nil, 0, 0
	}
	keys := make([]string, 0, len(builders))
	for key := range builders {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	objects := make([]nativeObjectSegment, 0, len(keys))
	totalDocs := int64(0)
	totalTokens := int64(0)
	for _, key := range keys {
		seg := builders[key].builder.Build()
		if seg.DocCount == 0 {
			continue
		}
		objects = append(objects, nativeObjectSegment{
			key:             key,
			name:            builders[key].name,
			segment:         seg,
			applyTombstones: true,
		})
		totalDocs += seg.DocCount
		totalTokens += seg.TokenSum
	}
	return objects, totalDocs, totalTokens
}

func hasDatalinkPart(tableDef *pbplan.TableDef, parts []string) bool {
	for _, part := range parts {
		idx, ok := tableDef.Name2ColIndex[part]
		if !ok {
			continue
		}
		if types.T(tableDef.Cols[idx].Typ.Id) == types.T_datalink {
			return true
		}
	}
	return false
}

func parseQualifiedTableName(name string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(name), ".")
	if len(parts) != 2 {
		return "", "", moerr.NewInternalErrorNoCtx("invalid fulltext table name")
	}
	return trimQuotedIdent(parts[0]), trimQuotedIdent(parts[1]), nil
}

func trimQuotedIdent(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

func decodeNativePK(raw []byte, typ types.T) any {
	v := types.DecodeValue(raw, typ)
	if bs, ok := v.([]byte); ok {
		return string(bs)
	}
	return v
}

func nativeRowKey(objKey string, ref ftnative.RowRef) string {
	return nativeBlockKey(objKey, ref.Block) + "#" + strconv.FormatUint(uint64(ref.Row), 10)
}

func nativeBlockKey(objKey string, blk uint16) string {
	return objKey + "#" + strconv.FormatUint(uint64(blk), 10)
}

func nativeCloneSet(src nativeDocSet) nativeDocSet {
	dst := make(nativeDocSet, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

func nativeUnion(dst, src nativeDocSet) {
	for k := range src {
		dst[k] = struct{}{}
	}
}

func nativeIntersect(dst, src nativeDocSet) nativeDocSet {
	ret := make(nativeDocSet)
	for k := range dst {
		if _, ok := src[k]; ok {
			ret[k] = struct{}{}
		}
	}
	return ret
}

func nativeDifference(dst, src nativeDocSet) {
	for k := range src {
		delete(dst, k)
	}
}
