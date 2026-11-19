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

package bootstrap

import (
	"bufio"
	"bytes"
	"encoding/json"

	"io"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

const (
	// SnapshotUpgradeDecisionPath is the shared operator input for catalog 4.0.7.
	SnapshotUpgradeDecisionPath = "SHARED:/catalog-upgrade/4.0.7/decisions.ndjson"
	snapshotDecisionFormat      = "matrixone-catalog-snapshot-decisions-v1"
	snapshotDecisionRecordLimit = 256 << 10
)

type snapshotDecisionEdge struct {
	TableID  *string `json:"table_id"`
	ParentID *string `json:"parent_id"`
	CloneTS  *string `json:"clone_ts"`
	Creator  *string `json:"creator"`
	Level    *string `json:"level"`
	Deleted  *bool   `json:"table_deleted"`
}

// Nullable source columns deliberately remain pointers: null and an empty
// string are different preimages and must not authorize the same decision.
type snapshotDecisionPreimage struct {
	CatalogAccountID string                `json:"catalog_account_id"`
	SnapshotID       string                `json:"snapshot_id"`
	Name             string                `json:"name"`
	Timestamp        *string               `json:"timestamp"`
	Level            *string               `json:"level"`
	AccountName      *string               `json:"account_name"`
	DatabaseName     *string               `json:"database_name"`
	TableName        *string               `json:"table_name"`
	ObjectID         *string               `json:"object_id"`
	PriorKind        string                `json:"prior_kind"`
	Edge             *snapshotDecisionEdge `json:"edge"`
}

type snapshotDecisionRecord struct {
	snapshotDecisionPreimage
	Decision string `json:"decision"`
}

type snapshotDecisionHeader struct {
	Format string `json:"format"`
	Target string `json:"target"`
}

type snapshotDecisionReader struct {
	scanner      *bufio.Scanner
	previousName string
	line         int
}

func newSnapshotDecisionReader(input io.Reader) (*snapshotDecisionReader, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), snapshotDecisionRecordLimit)
	reader := &snapshotDecisionReader{scanner: scanner}
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, wrapUpgradeError(err, "decision header")
		}
		return nil, moerr.NewInvalidInputNoCtx("decision file is empty")
	}
	reader.line = 1
	var header snapshotDecisionHeader
	if err := decodeDecisionJSON(scanner.Bytes(), &header); err != nil {
		return nil, wrapUpgradeError(err, "decision header")
	}
	if header.Format != snapshotDecisionFormat || header.Target != "4.0.7" {
		return nil, moerr.NewInvalidInputNoCtx("invalid snapshot decision format or target")
	}
	return reader, nil
}

func (r *snapshotDecisionReader) next() (*snapshotDecisionRecord, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, wrapUpgradeError(err, "decision line %d", r.line+1)
		}
		return nil, nil
	}
	r.line++
	var d snapshotDecisionRecord
	if err := decodeDecisionJSON(r.scanner.Bytes(), &d); err != nil {
		return nil, wrapUpgradeError(err, "decision line %d", r.line)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.scanner.Bytes(), &fields); err != nil {
		return nil, wrapUpgradeError(err, "decision line %d", r.line)
	}
	for _, name := range []string{"catalog_account_id", "snapshot_id", "name", "timestamp", "level", "account_name", "database_name", "table_name", "object_id", "prior_kind", "edge", "decision"} {
		if _, ok := fields[name]; !ok {
			return nil, moerr.NewInvalidInputNoCtxf("decision line %d: missing %s", r.line, name)
		}
	}
	if err := d.validate(); err != nil {
		return nil, wrapUpgradeError(err, "decision line %d", r.line)
	}
	if r.previousName != "" && d.Name <= r.previousName {
		return nil, moerr.NewInvalidInputNoCtxf("decision line %d: snapshot names must be strictly ordered", r.line)
	}
	r.previousName = d.Name
	return &d, nil
}

func (d *snapshotDecisionRecord) validate() error {
	if d.CatalogAccountID != "0" || d.PriorKind != "user" || d.Level == nil || *d.Level != "table" {
		return moerr.NewInvalidInputNoCtx("decision must refer to a system table snapshot previously marked user")
	}
	parsed, err := uuid.Parse(d.SnapshotID)
	if err != nil || parsed.String() != d.SnapshotID {
		return moerr.NewInvalidInputNoCtx("snapshot_id must be a canonical UUID")
	}
	child, ok := databranchutils.ParseBranchSnapshotName(d.Name)
	if !ok || databranchutils.BranchSnapshotName(child) != d.Name {
		return moerr.NewInvalidInputNoCtx("snapshot name is not a canonical branch name")
	}
	if d.Decision != "user" && d.Decision != "branch" {
		return moerr.NewInvalidInputNoCtx("decision must be user or branch")
	}
	if d.Timestamp != nil {
		if err := validateSignedDecimal(*d.Timestamp); err != nil {
			return wrapUpgradeError(err, "timestamp")
		}
	}
	if d.ObjectID != nil {
		if err := validateUnsignedDecimal(*d.ObjectID); err != nil {
			return wrapUpgradeError(err, "object_id")
		}
	}
	if d.Edge != nil {
		e := d.Edge
		if e.TableID == nil || e.ParentID == nil || e.CloneTS == nil || e.Creator == nil || e.Level == nil || e.Deleted == nil {
			return moerr.NewInvalidInputNoCtx("all edge fields are required")
		}
		if *e.TableID != strconv.FormatUint(child, 10) {
			return moerr.NewInvalidInputNoCtx("edge table_id does not match snapshot name")
		}
		for _, value := range []string{*e.TableID, *e.ParentID, *e.Creator} {
			if err := validateUnsignedDecimal(value); err != nil {
				return wrapUpgradeError(err, "edge ID")
			}
		}
		if err := validateSignedDecimal(*e.CloneTS); err != nil {
			return wrapUpgradeError(err, "edge clone_ts")
		}
	}
	return nil
}

func validateUnsignedDecimal(s string) error {
	value, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != s {
		return moerr.NewInvalidInputNoCtx("expected canonical unsigned decimal string")
	}
	return nil
}
func validateSignedDecimal(s string) error {
	value, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(value, 10) != s {
		return moerr.NewInvalidInputNoCtx("expected canonical signed decimal string")
	}
	return nil
}

// The database row is the authority; only the kind may differ on retry after a
// branch decision was committed in an earlier page.
func validateSnapshotDecision(d snapshotDecisionRecord, actual snapshotDecisionPreimage, actualKind string) error {
	if !reflect.DeepEqual(d.snapshotDecisionPreimage, actual) {
		return moerr.NewInvalidInputNoCtx("snapshot or branch-edge preimage changed")
	}
	switch actualKind {
	case "user":
		return nil
	case "branch":
		if d.Decision == "branch" {
			return nil
		}
		return moerr.NewInvalidInputNoCtx("cannot reverse a committed branch decision")
	default:
		return moerr.NewInvalidInputNoCtxf("unexpected snapshot kind %q", actualKind)
	}
}

// JSON's standard decoder accepts duplicate keys. Reject them before decoding
// so two conflicting operator decisions cannot be interpreted by key order.
func decodeDecisionJSON(raw []byte, target any) error {
	if !utf8.Valid(raw) {
		return moerr.NewInvalidInputNoCtx("decision JSON is not UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := checkJSONValue(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return moerr.NewInvalidInputNoCtx("trailing JSON value")
		}
		return err
	}
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		if err == nil {
			return moerr.NewInvalidInputNoCtx("trailing JSON value")
		}
		return err
	}
	return nil
}

func checkJSONValue(dec *json.Decoder, depth int) error {
	if depth > 32 {
		return moerr.NewInvalidInputNoCtx("decision JSON nesting exceeds 32")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]struct{})
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return moerr.NewInvalidInputNoCtx("invalid JSON key")
			}
			// encoding/json matches struct fields case-insensitively. Accept
			// only the format's canonical lower-case ASCII names so an alias
			// cannot override an administrator's earlier decision.
			for _, ch := range key {
				if (ch < 'a' || ch > 'z') && ch != '_' {
					return moerr.NewInvalidInputNoCtxf("noncanonical JSON key %q", key)
				}
			}
			if _, found := keys[key]; found {
				return moerr.NewInvalidInputNoCtxf("duplicate JSON key %q", key)
			}
			keys[key] = struct{}{}
			if err := checkJSONValue(dec, depth+1); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return moerr.NewInvalidInputNoCtx("invalid JSON object")
		}
	case '[':
		for dec.More() {
			if err := checkJSONValue(dec, depth+1); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return moerr.NewInvalidInputNoCtx("invalid JSON array")
		}
	default:
		return moerr.NewInvalidInputNoCtx("unexpected JSON delimiter")
	}
	return nil
}
