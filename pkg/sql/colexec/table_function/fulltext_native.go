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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type nativePreparedScan struct {
	rel         engine.Relation
	pkType      types.T
	indexTable  string
	objects     []nativeObjectSegment
	tombstones  engine.Tombstoner
	snapshot    types.TS
	fs          fileservice.FileService
	totalDocs   int64
	totalTokens int64
}

type nativeObjectSegment struct {
	name    objectio.ObjectName
	segment *ftnative.Segment
}

type nativeDocState struct {
	pk     any
	docLen int32
	ref    ftnative.RowRef
	obj    objectio.ObjectName
	counts []uint16
}

type nativeDocSet map[string]struct{}

type nativeDeleteCache struct {
	hasBlock map[string]bool
	deleted  map[string]map[uint32]bool
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

	scan, ok, err := prepareNativeScan(proc, srctbl, tblname, u.param)
	if err != nil || !ok {
		return ok, err
	}
	applyNativeSegmentStats(u, s, scan)

	if s.Mode == int64(tree.FULLTEXT_NL) {
		return true, populatePhraseCompat(u, proc, s, scan, s.Pattern)
	}
	if len(s.Pattern) == 1 && s.Pattern[0].Operator == fulltext.PHRASE {
		return true, populatePhraseCompat(u, proc, s, scan, s.Pattern[0].Children)
	}
	return true, populateBooleanNative(u, proc, s, scan)
}

func nativeQuerySupported(s *fulltext.SearchAccum) bool {
	switch s.Mode {
	case int64(tree.FULLTEXT_NL):
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
) (*nativePreparedScan, bool, error) {
	if len(param.Parts) == 0 {
		return nil, false, nil
	}

	dbName, tableName, err := parseQualifiedTableName(srctbl)
	if err != nil {
		return nil, false, err
	}
	_, indexTableName, err := parseQualifiedTableName(tblname)
	if err != nil {
		return nil, false, err
	}

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbName, proc.GetTxnOperator())
	if err != nil {
		return nil, false, err
	}
	rel, err := db.Relation(proc.Ctx, tableName, nil)
	if err != nil {
		return nil, false, err
	}

	visibleInfos, err := rel.GetColumMetadataScanInfo(proc.Ctx, param.Parts[0], false)
	if err != nil {
		return nil, false, err
	}
	visible := make(map[string]struct{}, len(visibleInfos))
	for _, info := range visibleInfos {
		visible[info.ObjectName] = struct{}{}
	}

	stats, err := rel.GetNonAppendableObjectStats(proc.Ctx)
	if err != nil {
		return nil, false, err
	}
	if len(visible) != len(stats) {
		return nil, false, nil
	}

	objects := make([]nativeObjectSegment, 0, len(stats))
	totalDocs := int64(0)
	totalTokens := int64(0)
	for i := range stats {
		name := stats[i].ObjectName()
		if _, ok := visible[name.String()]; !ok {
			return nil, false, nil
		}
		seg, exists, err := ftnative.ReadSidecar(proc.Ctx, proc.Base.FileService, name, indexTableName)
		if err != nil {
			return nil, false, err
		}
		if !exists {
			return nil, false, nil
		}
		objects = append(objects, nativeObjectSegment{
			name:    name,
			segment: seg,
		})
		totalDocs += seg.DocCount
		totalTokens += seg.TokenSum
	}

	tombstones, err := rel.CollectTombstones(proc.Ctx, 0, engine.Policy_CollectAllTombstones)
	if err != nil {
		return nil, false, err
	}

	tableDef := rel.GetTableDef(proc.Ctx)
	pkColIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, false, moerr.NewInternalErrorNoCtx("native fulltext scan missing primary key column")
	}
	return &nativePreparedScan{
		rel:         rel,
		pkType:      types.T(tableDef.Cols[pkColIdx].Typ.Id),
		indexTable:  indexTableName,
		objects:     objects,
		tombstones:  tombstones,
		snapshot:    types.TimestampToTS(proc.GetTxnOperator().SnapshotTS()),
		fs:          proc.Base.FileService,
		totalDocs:   totalDocs,
		totalTokens: totalTokens,
	}, true, nil
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
			if deleted, err := isNativeDeleted(proc.Ctx, scan, cache, obj.name, match.Ref); err != nil {
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
				key := nativeRowKey(obj.name, posting.Ref)
				state := docs[key]
				if state == nil {
					state = &nativeDocState{
						pk:     decodeNativePK(posting.Ref.PK, scan.pkType),
						docLen: posting.DocLen,
						ref:    posting.Ref,
						obj:    obj.name,
						counts: make([]uint16, s.Nkeywords),
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
				phraseSets[phrase][nativeRowKey(obj.name, match.Ref)] = struct{}{}
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
		if deleted, err := isNativeDeleted(proc.Ctx, scan, cache, state.obj, state.ref); err != nil {
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
	}
	return nil
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
	obj objectio.ObjectName,
	ref ftnative.RowRef,
) (bool, error) {
	bid := objectio.NewBlockidWithObjectID(obj.ObjectId(), ref.Block)
	blockKey := nativeBlockKey(obj, ref.Block)
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

func nativeRowKey(obj objectio.ObjectName, ref ftnative.RowRef) string {
	return nativeBlockKey(obj, ref.Block) + "#" + strconv.FormatUint(uint64(ref.Row), 10)
}

func nativeBlockKey(obj objectio.ObjectName, blk uint16) string {
	return obj.String() + "#" + strconv.FormatUint(uint64(blk), 10)
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
