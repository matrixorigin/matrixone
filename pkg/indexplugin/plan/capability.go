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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// ClusterHasIndexProvenance reports whether every live service understands the widened index
// metadata shape, and is the gate for CREATING one.
//
// The v4_0_7 migration is gated the same way, but it only widens tables that ALREADY exist.
// CREATE INDEX is the other producer of a wide table, and it is not covered by that gate: during
// a rolling upgrade a new CN would otherwise create a six-column metadata table that an old CN is
// still serving, and the old writer's INSERT is POSITIONAL --
//
//	INSERT INTO meta VALUES ('id', 'chk', ts, size)
//
// -- so it fails on arity the moment it meets the wider table. A column DEFAULT cannot repair
// that: the statement supplies four values for six columns before any default is consulted.
//
// It is also what lets a widened table stand as proof that no un-upgraded reader is left, which
// is the premise the CDC tail frame rows rely on. Without this gate a freshly created wide table
// says nothing about the rest of the deployment.
//
// MOProtocolVersion is the deployment's rollout gate: it tracks the OLDEST live service and is
// lowered again for a rollback, so it is read live at each decision rather than memoized.
// Unavailable answers false -- the legacy shape is the one that works on both sides.
func ClusterHasIndexProvenance(ctx CompilerContext) bool {
	if ctx == nil {
		return false
	}
	proc := ctx.GetProcess()
	if proc == nil {
		return false
	}
	// ServiceRuntime returns nil for a service it has never seen (an unregistered service id,
	// which internal and test callers do reach), so this must be checked before the read.
	rt := runtime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion61
}

// ClusterSupportsFulltext2Analyzer reports whether every live service understands the
// fulltext2 token_filter analyzer (MORPCVersion129), and is the gate for CREATING a
// token_filter index.
//
// token_filter strips stopwords at every WRITE path (CREATE build, CDC tail, idxcron
// rebuild), so the index's invariant is that NO segment holds a stopword posting. An
// older CN that ran the async CDC tail build or a scheduled rebuild would NOT strip,
// leaking stopword postings into its segments. That is not merely a lost size reduction:
// the recorded per-document length counts every indexed token, so a non-stripping writer
// records a LARGER length for its documents, which feeds the BM25 length-normalization
// factor and the index-wide average length. It therefore perturbs the score of EVERY
// query served from the affected generation, not only a stopword query. Gating CREATE on
// the cluster-wide version ensures every CN that could build or maintain the index strips
// identically. The capability is a hard gate with no fallback: it must hold for the whole
// lifetime of a token_filter index (see the design doc's rollout/rollback section), and a
// node below it must not write or maintain one.
// (The READ side needs no gate: WAND/boolean scoring consults only the query's own terms,
// so a leaked stopword posting is unreachable unless the stopword is in the query — and a
// supporting reader strips it from the query. Query-side stripping is a score-neutral
// optimization; the write side is where the invariant must be enforced.)
// Unavailable answers false — the safe default is to reject the new option.
func ClusterSupportsFulltext2Analyzer(ctx CompilerContext) bool {
	if ctx == nil {
		return false
	}
	proc := ctx.GetProcess()
	if proc == nil {
		return false
	}
	rt := runtime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion129
}
