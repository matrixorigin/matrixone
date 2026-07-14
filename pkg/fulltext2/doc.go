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

// Package fulltext2 is the WAND-based positional fulltext engine selected by the
// FULLTEXT index parameter VERSION=2. It is a completely separate engine from
// classic v1 (generated-SQL + Go Eval) and from bm25 (position-free ranked
// retrieval); the fulltext plugin is a thin version-router that delegates each
// hook to v1 (verbatim) or here.
//
// It shares the index-plugin framework with bm25 — the plugin hooks, the ISCP
// CDC consumer, VectorIndexCache, idxcron MERGE/REBUILD, and the payload-
// agnostic CDC chunk framing — and reuses bm25's leaf serialization patterns
// (tar members, little-endian buffers, PK encode/decode). But the segment
// BACKBONE genuinely diverges from bm25 on three of four axes, so the engine is
// kept separate rather than sharing a lowest-common-denominator core:
//
//	                bm25 (unchanged)               fulltext v2 (here)
//	term dict       jieba dict-id + unordered      SORTED term dict of the actual
//	                overflow map                   indexed terms (mandatory for `word*`)
//	posting payload df / impact only               + POSITIONS (phrase, ngram reassembly)
//	query model     whole-sentence bag → WAND      parser → operator tree
//	scoring         BM25                           TF-IDF AND BM25
//
// # Segment format (§4 of fulltext2.md)
//
//	fulltext v2 segment  (tag=0 base sub  /  tag=1 CDC tail frame):
//	  ├─ term dict:  term(string) → { df, posting-offset }   ← SORTED; dictionary-free
//	  ├─ postings:   per term, doc-sorted, block-max (raw per-block max-TF) + POSITIONS
//	  ├─ docmap:     ord → { pk, docLen };  N = doc count
//	  └─ avgDocLen:  computed at LOAD (Σ docLen / N across all loaded segments)
//
// Dictionary-free by construction: the term dict holds exactly the terms that
// were indexed — gojieba words or ngram bigrams — with no jieba-dict dependency
// in the backbone. One sorted structure delivers term lookup AND prefix
// enumeration (the `word*` boolean operator). On disk it is a vellum FST
// (termdict.go) — a compact, minimized encoding of exactly this sorted mapping;
// byte-oriented, so UTF-8/CJK terms need no special handling (Go's string sort
// is already the FST's required ascending byte order).
//
// Block-max stores the RAW per-block max-TF (scorer-agnostic), never a baked
// impact, so one segment serves both TF-IDF and BM25; the active scorer derives
// its max-impact bound at query time.
//
// Build order of the package (see fulltext2.md phasing):
//   - P0.3 (here): the segment format — types, term-dict lookup/prefix backbone,
//     serialize/load on the shared CDC framing.
//   - P1: NL exact-phrase evaluator (positional two-phase) + TF-IDF/BM25 scorer.
//   - P2/P3: boolean operators + `word*` prefix.
//   - P4: CDC sink, sync build, idxcron MERGE/REBUILD, migration.
package fulltext2
