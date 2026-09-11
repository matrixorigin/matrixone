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

package mvdefinition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	Format = 1
	// MV jobs use a dedicated protocol fence.  Do not reuse an older
	// capability number: older CNs can otherwise accept the catalog definition
	// and later misinterpret the MV job as an index job.
	RequiredCapability = defines.MORPCVersion64
	Property           = "mv_definition"
	OwnerProperty      = "mv_owner"
	StatePrefix        = "__mo_mv_state_"
	MaxSources         = 16
	// Bounds catalog decoding before allocation; SQL statements already have a
	// finite admission size. This does not cap persistent source/result rows.
	MaxDefinitionBytes = 1 << 20
)

type Relation struct {
	Database   string `json:"database"`
	Name       string `json:"name"`
	DatabaseID uint64 `json:"database_id"`
	ID         uint64 `json:"id"`
}

type Source struct {
	Relation
	// Presence is significant: catalog version zero is valid.
	Version *uint32 `json:"version"`
}

type Definition struct {
	Format             int       `json:"format"`
	RequiredCapability int64     `json:"required_capability"`
	AccountID          uint32    `json:"account_id"`
	Target             Relation  `json:"target"`
	Generation         uint64    `json:"generation"`
	CreateSQL          string    `json:"create_sql"`
	RefreshSQL         string    `json:"refresh_sql"`
	Method             string    `json:"method"`
	Timing             string    `json:"timing"`
	Columns            []string  `json:"columns"`
	Sources            []Source  `json:"sources"`
	Incremental        string    `json:"incremental,omitempty"`
	State              *Relation `json:"state,omitempty"`
}

// Reference is the only authoritative MV payload in an ISCP job. SQL and state
// layouts belong to the target relation and are loaded in the refresh snapshot.
type Reference struct {
	Format     int
	TargetID   uint64
	Generation uint64
	Digest     string
}

type Owner struct {
	Format     int    `json:"format"`
	AccountID  uint32 `json:"account_id"`
	TargetID   uint64 `json:"target_id"`
	Generation uint64 `json:"generation"`
	StateID    uint64 `json:"state_id"`
}

type InvalidDefinition struct{ Reason string }

func (e *InvalidDefinition) Error() string {
	return "invalid materialized view dependency generation: " + e.Reason
}
func Invalid(format string, args ...any) error {
	return &InvalidDefinition{Reason: fmt.Sprintf(format, args...)}
}

func (d *Definition) Validate(final bool) error {
	if d == nil || d.Format != Format || d.RequiredCapability != RequiredCapability {
		return Invalid("unsupported definition format or capability")
	}
	if d.Generation == 0 || d.Target.Database == "" || d.Target.Name == "" || d.CreateSQL == "" || d.RefreshSQL == "" || len(d.Columns) == 0 {
		return Invalid("incomplete target definition")
	}
	if final && (d.Target.ID == 0 || d.Target.DatabaseID == 0) {
		return Invalid("missing target identity")
	}
	if d.Method != "fast" && d.Method != "force" && d.Method != "complete" {
		return Invalid("unsupported refresh method")
	}
	if d.Timing != "change" && d.Timing != "demand" || d.Timing == "demand" && d.Method != "complete" {
		return Invalid("unsupported refresh timing")
	}
	if len(d.Sources) == 0 || len(d.Sources) > MaxSources {
		return Invalid("unsupported source count")
	}
	seen := make(map[uint64]bool, len(d.Sources))
	for _, source := range d.Sources {
		if source.ID == 0 || source.DatabaseID == 0 || source.Name == "" || source.Database == "" || source.Version == nil || seen[source.ID] {
			return Invalid("missing or duplicate source identity")
		}
		seen[source.ID] = true
	}
	if d.Incremental != "" {
		desc, err := DecodeIncremental(d.Incremental)
		if err != nil {
			return Invalid("%v", err)
		}
		if desc.StateTable != "" && (d.State == nil || d.State.Name != desc.StateTable) {
			return Invalid("auxiliary state does not match incremental definition")
		}
	} else if d.Method == "fast" {
		return Invalid("FAST requires an incremental definition")
	}
	if d.State != nil && (d.State.Name == "" || d.State.Database != d.Target.Database || final && (d.State.ID == 0 || d.State.DatabaseID != d.Target.DatabaseID)) {
		return Invalid("invalid auxiliary state identity")
	}
	return nil
}

func Encode(d *Definition) (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	if len(b) > MaxDefinitionBytes {
		return "", Invalid("definition exceeds size limit")
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
func Decode(encoded string, final bool) (*Definition, error) {
	if encoded == "" || len(encoded) > base64.StdEncoding.EncodedLen(MaxDefinitionBytes) {
		return nil, Invalid("missing or oversized definition; recreate this view")
	}
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, Invalid("invalid encoding")
	}
	var d Definition
	if err = json.Unmarshal(b, &d); err != nil {
		return nil, Invalid("invalid definition JSON")
	}
	if err = d.Validate(final); err != nil {
		return nil, err
	}
	return &d, nil
}
func (d *Definition) Reference() (*Reference, error) {
	if err := d.Validate(true); err != nil {
		return nil, err
	}
	encoded, err := Encode(d)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(encoded))
	return &Reference{Format: Format, TargetID: d.Target.ID, Generation: d.Generation, Digest: hex.EncodeToString(sum[:])}, nil
}
func (r *Reference) Validate() error {
	if r == nil || r.Format != Format || r.TargetID == 0 || r.Generation == 0 || len(r.Digest) != sha256.Size*2 {
		return Invalid("invalid job reference")
	}
	if _, err := hex.DecodeString(r.Digest); err != nil {
		return Invalid("invalid definition digest")
	}
	return nil
}
func (d *Definition) Match(r *Reference) error {
	if err := r.Validate(); err != nil {
		return err
	}
	actual, err := d.Reference()
	if err != nil {
		return err
	}
	if *actual != *r {
		return Invalid("target definition was replaced")
	}
	return nil
}

func PropertyValue(def *plan.TableDef, key string) string {
	if def == nil {
		return ""
	}
	for _, p := range def.Props {
		if p.Key == key {
			return p.Value
		}
	}
	for _, item := range def.Defs {
		if props := item.GetProperties(); props != nil {
			for _, p := range props.Properties {
				if p.Key == key {
					return p.Value
				}
			}
		}
	}
	return ""
}
func FromTable(def *plan.TableDef) (*Definition, error) {
	if def == nil || def.TableType != "m" {
		return nil, Invalid("unauthenticated physical view identity")
	}
	d, err := Decode(PropertyValue(def, Property), true)
	if err != nil {
		return nil, err
	}
	if def.TblId != d.Target.ID || def.DbId != d.Target.DatabaseID || def.DbName != d.Target.Database || def.Name != d.Target.Name {
		return nil, Invalid("target relation identity changed")
	}
	return d, nil
}
func IsReservedProperty(key string) bool { return strings.HasPrefix(strings.ToLower(key), "mv_") }
func IsStateName(name string) bool       { return strings.HasPrefix(strings.ToLower(name), StatePrefix) }

func EncodeOwner(owner Owner) string { b, _ := json.Marshal(owner); return string(b) }
func StateOwner(def *plan.TableDef) (*Owner, error) {
	if def == nil || def.TableType != "i" {
		return nil, Invalid("unauthenticated auxiliary view identity")
	}
	var owner Owner
	if err := json.Unmarshal([]byte(PropertyValue(def, OwnerProperty)), &owner); err != nil || owner.Format != Format || owner.TargetID == 0 || owner.Generation == 0 || owner.StateID == 0 || owner.StateID != def.GetTblId() {
		return nil, Invalid("invalid auxiliary ownership")
	}
	return &owner, nil
}

// ValidateSources uses the caller's catalog snapshot. Names never repair a
// missing ID or version: even a schema-compatible replacement is a new source.
func (d *Definition) ValidateSources(resolve func(Source) (*plan.TableDef, error)) error {
	for _, source := range d.Sources {
		def, err := resolve(source)
		if err != nil {
			return Invalid("source %s.%s is unavailable: %v", source.Database, source.Name, err)
		}
		if source.Version == nil || def == nil || def.TblId != source.ID || def.DbId != source.DatabaseID || def.Version != *source.Version || def.Name != source.Name || def.DbName != source.Database || def.TableType != "r" || def.IsTemporary {
			return Invalid("source %s.%s changed; recreate this view", source.Database, source.Name)
		}
	}
	return nil
}

type authorityKey struct{}
type Authority struct {
	AccountID                     uint32
	TargetID, Generation, StateID uint64
}

func WithAuthority(ctx context.Context, d *Definition) context.Context {
	a := Authority{AccountID: d.AccountID, TargetID: d.Target.ID, Generation: d.Generation}
	if d.State != nil {
		a.StateID = d.State.ID
	}
	return context.WithValue(ctx, authorityKey{}, a)
}
func CanWrite(ctx context.Context, def *plan.TableDef) bool {
	if ctx == nil || def == nil {
		return false
	}
	a, ok := ctx.Value(authorityKey{}).(Authority)
	accountID, err := defines.GetAccountId(ctx)
	if !ok || a.TargetID == 0 || err != nil || accountID != a.AccountID {
		return false
	}
	if def.TblId == a.TargetID {
		d, err := FromTable(def)
		return err == nil && d.AccountID == a.AccountID && d.Generation == a.Generation
	}
	if def.TblId == a.StateID && a.StateID != 0 {
		owner, err := StateOwner(def)
		return err == nil && owner.AccountID == a.AccountID && owner.TargetID == a.TargetID && owner.Generation == a.Generation
	}
	return false
}

// PlannerKind leaves the durable view-kind barrier in the catalog while new
// binaries use the physical relation. Old binaries keep ViewSql and reject it.
func PlannerKind(def *plan.TableDef) {
	if def == nil || def.TableType != "v" || def.ViewSql == nil {
		return
	}
	var view struct{ Stmt string }
	if json.Unmarshal([]byte(def.ViewSql.View), &view) != nil || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(view.Stmt)), "create materialized view ") {
		return
	}
	if PropertyValue(def, Property) != "" {
		def.TableType = "m"
		def.ViewSql = nil
	} else if PropertyValue(def, OwnerProperty) != "" {
		def.TableType = "i"
		def.ViewSql = nil
	}
}

// StateCreation grants only CREATE of the named auxiliary relation before its
// catalog ID exists. Data writes still require the finalized exact-ID authority.
type stateCreationKey struct{}
type StateCreation struct {
	Owner                   Owner
	Database, Name, ViewSQL string
}

func WithStateCreation(ctx context.Context, c StateCreation) context.Context {
	return context.WithValue(ctx, stateCreationKey{}, c)
}
func GetStateCreation(ctx context.Context, database, name string) (StateCreation, bool) {
	if ctx == nil {
		return StateCreation{}, false
	}
	c, ok := ctx.Value(stateCreationKey{}).(StateCreation)
	return c, ok && c.Owner.TargetID != 0 && c.Owner.Generation != 0 && c.Database == database && c.Name == name && IsStateName(name)
}
