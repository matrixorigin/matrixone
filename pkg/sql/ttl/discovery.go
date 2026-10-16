// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	maxDiscoveryNameBytes   = 256
	maxDiscoveryErrors      = 8
	maxDiscoveryErrorDetail = 128
	candidateFixedCredit    = 512
	userTableFilter         = "relkind = 'r' AND reldatabase NOT IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') AND partitioned = 0"
)

// DiscoveryOptions may lower, but never raise, the worker's resource ceilings.
type DiscoveryOptions struct {
	PageSize           int
	MaxPages           int
	MaxCandidates      int
	MaxCandidateBytes  int
	MaxConstraintBytes int
}

func DefaultDiscoveryOptions() DiscoveryOptions {
	return DiscoveryOptions{PageSize: 128, MaxPages: 4, MaxCandidates: 32, MaxCandidateBytes: 128 << 10, MaxConstraintBytes: 16 << 10}
}

func (o DiscoveryOptions) validate() error {
	max := DefaultDiscoveryOptions()
	if o.PageSize <= 0 || o.PageSize > max.PageSize || o.MaxPages <= 0 || o.MaxPages > max.MaxPages ||
		o.MaxCandidates <= 0 || o.MaxCandidates > max.MaxCandidates || o.MaxCandidateBytes < candidateFixedCredit || o.MaxCandidateBytes > max.MaxCandidateBytes ||
		o.MaxConstraintBytes <= 0 || o.MaxConstraintBytes > max.MaxConstraintBytes {
		return moerr.NewInvalidInputNoCtx("invalid TTL discovery limits")
	}
	return nil
}

type discoveryKey struct {
	account uint32
	table   uint64
}

func (k discoveryKey) after(other discoveryKey) bool {
	return k.account > other.account || (k.account == other.account && k.table > other.table)
}

type discoveryCursor struct {
	after, upper discoveryKey
	active       bool
}
type discoveryWindow struct {
	candidates  []Candidate
	last, upper discoveryKey
	exhausted   bool
}

type discoveryDiagnostics struct {
	samples []string
	omitted int
}

func (d *discoveryDiagnostics) add(account uint32, table uint64, detail string) {
	if len(d.samples) == maxDiscoveryErrors {
		d.omitted++
		return
	}
	if len(detail) > maxDiscoveryErrorDetail {
		detail = detail[:maxDiscoveryErrorDetail]
	}
	// Formatting copies the truncated detail rather than retaining a large error
	// message or its original joined error/metadata graph.
	d.samples = append(d.samples, fmt.Sprintf("TTL account %d table %d: %s", account, table, detail))
}

func (d *discoveryDiagnostics) err() error {
	if len(d.samples) == 0 {
		return nil
	}
	message := strings.Join(d.samples, "; ")
	if d.omitted != 0 {
		message += fmt.Sprintf("; %d further TTL errors omitted", d.omitted)
	}
	return moerr.NewInternalErrorNoCtxf("%s", message)
}

// Discover returns one bounded window, not the entire catalog. Scheduled work
// retains its fenced continuation through TaskExecutor; this helper starts a
// fresh cycle for internal callers.
func Discover(ctx context.Context, exec executor.SQLExecutor) ([]Candidate, error) {
	var diagnostics discoveryDiagnostics
	w, err := discoverWindow(ctx, exec, discoveryCursor{}, DefaultDiscoveryOptions(), &diagnostics)
	if err != nil {
		return nil, err
	}
	return w.candidates, diagnostics.err()
}

func discoveryUpper(ctx context.Context, exec executor.SQLExecutor) (discoveryKey, bool, error) {
	if err := ctx.Err(); err != nil {
		return discoveryKey{}, false, err
	}
	res, err := exec.Exec(ctx, "SELECT account_id, rel_id FROM mo_catalog.mo_tables WHERE "+userTableFilter+" ORDER BY account_id DESC, rel_id DESC LIMIT 1", executor.Options{}.WithAccountID(catalog.System_Account).WithTimeZone(time.UTC))
	if err != nil {
		res.Close()
		return discoveryKey{}, false, err
	}
	defer res.Close()
	if err := ctx.Err(); err != nil {
		return discoveryKey{}, false, err
	}
	var key discoveryKey
	count := 0
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		count += rows
		if count != 1 || len(cols) != 2 {
			return false
		}
		key = discoveryKey{vector.MustFixedColNoTypeCheck[uint32](cols[0])[0], vector.MustFixedColNoTypeCheck[uint64](cols[1])[0]}
		return true
	})
	if err := ctx.Err(); err != nil {
		return discoveryKey{}, false, err
	}
	if count > 1 || (count == 1 && key == (discoveryKey{})) {
		return discoveryKey{}, false, moerr.NewInternalErrorNoCtx("invalid TTL discovery upper result")
	}
	return key, count != 0, nil
}

func discoverySQL(after, upper discoveryKey, o DiscoveryOptions) string {
	return fmt.Sprintf("SELECT account_id, rel_id, rel_version, CASE WHEN LENGTH(reldatabase) <= %d THEN reldatabase ELSE '' END, CASE WHEN LENGTH(relname) <= %d THEN relname ELSE '' END, CASE WHEN LENGTH(`constraint`) <= %d THEN `constraint` ELSE NULL END, LENGTH(`constraint`) FROM mo_catalog.mo_tables WHERE %s AND (account_id > %d OR (account_id = %d AND rel_id > %d)) AND (account_id < %d OR (account_id = %d AND rel_id <= %d)) ORDER BY account_id, rel_id LIMIT %d", maxDiscoveryNameBytes, maxDiscoveryNameBytes, o.MaxConstraintBytes, userTableFilter, after.account, after.account, after.table, upper.account, upper.account, upper.table, o.PageSize)
}

func candidateCredit(data []byte, database, table, column string) int {
	// The doubled payload credit covers allocation-class rounding; fixed credit
	// covers candidate/slice growth and string headers on supported platforms.
	return candidateFixedCredit + 2*(len(data)+len(database)+len(table)+len(column))
}

func discoverWindow(ctx context.Context, exec executor.SQLExecutor, cursor discoveryCursor, o DiscoveryOptions, diagnostics *discoveryDiagnostics) (discoveryWindow, error) {
	if err := o.validate(); err != nil {
		return discoveryWindow{}, err
	}
	if err := ctx.Err(); err != nil {
		return discoveryWindow{}, err
	}
	if !cursor.active {
		upper, found, err := discoveryUpper(ctx, exec)
		if err != nil {
			return discoveryWindow{}, err
		}
		if !found {
			return discoveryWindow{exhausted: true}, nil
		}
		cursor.upper, cursor.active = upper, true
	}
	w := discoveryWindow{last: cursor.after, upper: cursor.upper}
	used := 0
	for page := 0; page < o.MaxPages; page++ {
		candidates, last, exhausted, stopped, err := discoverPage(ctx, exec, w.last, w.upper, o, o.MaxCandidates-len(w.candidates), o.MaxCandidateBytes-used, diagnostics)
		if err != nil {
			return discoveryWindow{}, err
		}
		for _, c := range candidates {
			used += candidateCredit(c.Constraint, c.Database, c.Table, c.Policy.Column)
		}
		w.candidates = append(w.candidates, candidates...)
		w.last, w.exhausted = last, exhausted
		if exhausted || stopped || len(w.candidates) == o.MaxCandidates {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return discoveryWindow{}, err
	}
	return w, nil
}

func discoverPage(ctx context.Context, exec executor.SQLExecutor, after, upper discoveryKey, o DiscoveryOptions, countRoom, byteRoom int, diagnostics *discoveryDiagnostics) ([]Candidate, discoveryKey, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, after, false, false, err
	}
	res, err := exec.Exec(ctx, discoverySQL(after, upper, o), executor.Options{}.WithAccountID(catalog.System_Account).WithTimeZone(time.UTC))
	if err != nil {
		res.Close()
		return nil, after, false, false, err
	}
	defer res.Close()
	if err := ctx.Err(); err != nil {
		return nil, after, false, false, err
	}
	var candidates []Candidate
	last, seen, stopped := after, 0, false
	var decodeErr error
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if err := ctx.Err(); err != nil {
			decodeErr = err
			return false
		}
		if len(cols) != 7 || rows > o.PageSize-seen {
			decodeErr = moerr.NewInternalErrorNoCtx("invalid TTL discovery page result")
			return false
		}
		seen += rows
		for i := 0; i < rows; i++ {
			if err := ctx.Err(); err != nil {
				decodeErr = err
				return false
			}
			if len(candidates) == countRoom {
				stopped = true
				return false
			}
			key := discoveryKey{vector.GetFixedAtNoTypeCheck[uint32](cols[0], i), vector.GetFixedAtNoTypeCheck[uint64](cols[1], i)}
			if !key.after(last) || key.after(upper) {
				decodeErr = moerr.NewInternalErrorNoCtx("invalid TTL discovery key order")
				return false
			}
			var data []byte
			if !cols[5].IsNull(uint64(i)) {
				data = cols[5].GetBytesAt(i)
			}
			length := int64(0)
			if !cols[6].IsNull(uint64(i)) {
				length = vector.GetFixedAtNoTypeCheck[int64](cols[6], i)
			}
			database, table := "", ""
			if !cols[3].IsNull(uint64(i)) {
				database = cols[3].UnsafeGetStringAt(i)
			}
			if !cols[4].IsNull(uint64(i)) {
				table = cols[4].UnsafeGetStringAt(i)
			}
			if length < 0 || length > int64(o.MaxConstraintBytes) || len(data) > o.MaxConstraintBytes || database == "" || table == "" || len(database) > maxDiscoveryNameBytes || len(table) > maxDiscoveryNameBytes {
				diagnostics.add(key.account, key.table, "metadata or name exceeds discovery bounds")
				last = key
				continue
			}
			p, err := policyFromConstraint(ctx, data)
			if err := ctx.Err(); err != nil {
				decodeErr = err
				return false
			}
			if err != nil {
				diagnostics.add(key.account, key.table, "invalid TTL metadata")
				last = key
				continue
			}
			if p == nil {
				last = key
				continue
			}
			credit := candidateCredit(data, database, table, p.Column)
			if credit > o.MaxCandidateBytes {
				diagnostics.add(key.account, key.table, "candidate exceeds discovery credit")
				last = key
				continue
			}
			if credit > byteRoom {
				stopped = true
				return false
			}
			byteRoom -= credit
			candidates = append(candidates, Candidate{AccountID: key.account, TableID: key.table, Version: vector.GetFixedAtNoTypeCheck[uint32](cols[2], i), Database: strings.Clone(database), Table: strings.Clone(table), Constraint: bytes.Clone(data), Policy: *p})
			last = key
		}
		return true
	})
	if err := ctx.Err(); err != nil {
		return nil, after, false, false, err
	}
	if decodeErr != nil {
		return nil, after, false, false, decodeErr
	}
	return candidates, last, !stopped && (seen < o.PageSize || last == upper), stopped, nil
}
