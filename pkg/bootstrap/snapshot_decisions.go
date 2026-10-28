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
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/google/uuid"
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
			return nil, fmt.Errorf("decision header: %w", err)
		}
		return nil, errors.New("decision file is empty")
	}
	reader.line = 1
	var header snapshotDecisionHeader
	if err := decodeDecisionJSON(scanner.Bytes(), &header); err != nil {
		return nil, fmt.Errorf("decision header: %w", err)
	}
	if header.Format != snapshotDecisionFormat || header.Target != "4.0.7" {
		return nil, errors.New("invalid snapshot decision format or target")
	}
	return reader, nil
}

func (r *snapshotDecisionReader) next() (*snapshotDecisionRecord, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("decision line %d: %w", r.line+1, err)
		}
		return nil, nil
	}
	r.line++
	var d snapshotDecisionRecord
	if err := decodeDecisionJSON(r.scanner.Bytes(), &d); err != nil {
		return nil, fmt.Errorf("decision line %d: %w", r.line, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.scanner.Bytes(), &fields); err != nil {
		return nil, fmt.Errorf("decision line %d: %w", r.line, err)
	}
	for _, name := range []string{"catalog_account_id", "snapshot_id", "name", "timestamp", "level", "account_name", "database_name", "table_name", "object_id", "prior_kind", "edge", "decision"} {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("decision line %d: missing %s", r.line, name)
		}
	}
	if err := d.validate(); err != nil {
		return nil, fmt.Errorf("decision line %d: %w", r.line, err)
	}
	if r.previousName != "" && d.Name <= r.previousName {
		return nil, fmt.Errorf("decision line %d: snapshot names must be strictly ordered", r.line)
	}
	r.previousName = d.Name
	return &d, nil
}

func (d *snapshotDecisionRecord) validate() error {
	if d.CatalogAccountID != "0" || d.PriorKind != "user" || d.Level == nil || *d.Level != "table" {
		return errors.New("decision must refer to a system table snapshot previously marked user")
	}
	parsed, err := uuid.Parse(d.SnapshotID)
	if err != nil || parsed.String() != d.SnapshotID {
		return errors.New("snapshot_id must be a canonical UUID")
	}
	child, ok := databranchutils.ParseBranchSnapshotName(d.Name)
	if !ok || databranchutils.BranchSnapshotName(child) != d.Name {
		return errors.New("snapshot name is not a canonical branch name")
	}
	if d.Decision != "user" && d.Decision != "branch" {
		return errors.New("decision must be user or branch")
	}
	if d.Timestamp != nil {
		if err := validateSignedDecimal(*d.Timestamp); err != nil {
			return fmt.Errorf("timestamp: %w", err)
		}
	}
	if d.ObjectID != nil {
		if err := validateUnsignedDecimal(*d.ObjectID); err != nil {
			return fmt.Errorf("object_id: %w", err)
		}
	}
	if d.Edge != nil {
		e := d.Edge
		if e.TableID == nil || e.ParentID == nil || e.CloneTS == nil || e.Creator == nil || e.Level == nil || e.Deleted == nil {
			return errors.New("all edge fields are required")
		}
		if *e.TableID != strconv.FormatUint(child, 10) {
			return errors.New("edge table_id does not match snapshot name")
		}
		for _, value := range []string{*e.TableID, *e.ParentID, *e.Creator} {
			if err := validateUnsignedDecimal(value); err != nil {
				return fmt.Errorf("edge ID: %w", err)
			}
		}
		if err := validateSignedDecimal(*e.CloneTS); err != nil {
			return fmt.Errorf("edge clone_ts: %w", err)
		}
	}
	return nil
}

func validateUnsignedDecimal(s string) error {
	value, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != s {
		return errors.New("expected canonical unsigned decimal string")
	}
	return nil
}
func validateSignedDecimal(s string) error {
	value, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(value, 10) != s {
		return errors.New("expected canonical signed decimal string")
	}
	return nil
}

// The database row is the authority; only the kind may differ on retry after a
// branch decision was committed in an earlier page.
func validateSnapshotDecision(d snapshotDecisionRecord, actual snapshotDecisionPreimage, actualKind string) error {
	if !reflect.DeepEqual(d.snapshotDecisionPreimage, actual) {
		return errors.New("snapshot or branch-edge preimage changed")
	}
	switch actualKind {
	case "user":
		return nil
	case "branch":
		if d.Decision == "branch" {
			return nil
		}
		return errors.New("cannot reverse a committed branch decision")
	default:
		return fmt.Errorf("unexpected snapshot kind %q", actualKind)
	}
}

// JSON's standard decoder accepts duplicate keys. Reject them before decoding
// so two conflicting operator decisions cannot be interpreted by key order.
func decodeDecisionJSON(raw []byte, target any) error {
	if !utf8.Valid(raw) {
		return errors.New("decision JSON is not UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := checkJSONValue(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
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
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func checkJSONValue(dec *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("decision JSON nesting exceeds 32")
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
				return errors.New("invalid JSON key")
			}
			// encoding/json matches struct fields case-insensitively. Accept
			// only the format's canonical lower-case ASCII names so an alias
			// cannot override an administrator's earlier decision.
			for _, ch := range key {
				if (ch < 'a' || ch > 'z') && ch != '_' {
					return fmt.Errorf("noncanonical JSON key %q", key)
				}
			}
			if _, found := keys[key]; found {
				return fmt.Errorf("duplicate JSON key %q", key)
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
			return errors.New("invalid JSON object")
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
			return errors.New("invalid JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}
