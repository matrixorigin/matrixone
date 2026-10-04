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

package ref

import (
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type Type string

const (
	TypeBranch   Type = "branch"
	TypeTag      Type = "tag"
	TypeHash     Type = "hash"
	TypeSnapshot Type = "snapshot"
)

type Spec struct {
	Name       string
	Type       Type
	SnapshotID int64
	Hash       string
	ReadOnly   bool
}

func ParseNessieRef(raw string, meta *api.TableMetadata) (Spec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = model.DefaultRefMain
	}
	if typ, value, ok := splitTypedRef(raw); ok {
		return parseTypedRef(typ, value)
	}
	if meta != nil && meta.Refs != nil {
		if snapshotRef, ok := meta.Refs[raw]; ok {
			return FromSnapshotRef(raw, snapshotRef), nil
		}
	}
	return Spec{Name: raw, Type: TypeBranch}, nil
}

func FromSnapshotRef(name string, snapshotRef api.SnapshotRef) Spec {
	typ := Type(strings.ToLower(strings.TrimSpace(snapshotRef.Type)))
	if typ == "" {
		typ = TypeBranch
	}
	return Spec{
		Name:       strings.TrimSpace(name),
		Type:       typ,
		SnapshotID: snapshotRef.SnapshotID,
		ReadOnly:   typ == TypeTag,
	}
}

func ValidateWrite(spec Spec, caps api.CatalogCapabilities, allowTagMove bool) error {
	switch spec.Type {
	case "", TypeBranch:
		return nil
	case TypeTag:
		if caps.BranchTag && allowTagMove {
			return nil
		}
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg tag refs are read-only for writes", map[string]string{"ref": spec.Name})
	case TypeHash, TypeSnapshot:
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg pinned refs are read-only for writes", map[string]string{
			"ref":  spec.Name,
			"type": string(spec.Type),
		})
	default:
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg ref type is unsupported", map[string]string{"type": string(spec.Type)})
	}
}

func ApplyToAppendRequest(req api.AppendRequest, spec Spec, caps api.CatalogCapabilities, allowTagMove bool) (api.AppendRequest, error) {
	if err := ValidateWrite(spec, caps, allowTagMove); err != nil {
		return api.AppendRequest{}, err
	}
	if strings.TrimSpace(spec.Name) != "" {
		req.TargetRef = spec.Name
	}
	return req, nil
}

func CommitRequirement(spec Spec, baseSnapshotID int64) api.CommitRequirement {
	refName := strings.TrimSpace(spec.Name)
	if refName == "" {
		refName = model.DefaultRefMain
	}
	if baseSnapshotID == 0 {
		return api.CommitRequirement{Type: "assert-ref-snapshot-id", Ref: refName}
	}
	return api.CommitRequirement{Type: "assert-ref-snapshot-id", Ref: refName, SnapshotID: baseSnapshotID}
}

func RefreshCache(accountID uint32, catalogID uint64, namespace, table string, meta *api.TableMetadata, source string, now time.Time) []model.RefCache {
	if meta == nil || len(meta.Refs) == 0 {
		return nil
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = "catalog"
	}
	out := make([]model.RefCache, 0, len(meta.Refs))
	for name, snapshotRef := range meta.Refs {
		spec := FromSnapshotRef(name, snapshotRef)
		out = append(out, model.RefCache{
			AccountID:  accountID,
			CatalogID:  catalogID,
			Namespace:  namespace,
			TableName:  table,
			RefName:    name,
			RefType:    string(spec.Type),
			SnapshotID: strconv.FormatInt(snapshotRef.SnapshotID, 10),
			LastSeenAt: now,
			Source:     source,
			CreatedAt:  now,
			UpdatedAt:  now,
			Version:    1,
		})
	}
	return out
}

func splitTypedRef(raw string) (string, string, bool) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	typ := strings.ToLower(strings.TrimSpace(parts[0]))
	value := strings.TrimSpace(parts[1])
	switch Type(typ) {
	case TypeBranch, TypeTag, TypeHash, TypeSnapshot:
		return typ, value, true
	default:
		return "", "", false
	}
}

func parseTypedRef(typ, value string) (Spec, error) {
	if value == "" {
		return Spec{}, api.NewError(api.ErrConfigInvalid, "Iceberg typed ref requires a value", map[string]string{"type": typ})
	}
	switch Type(typ) {
	case TypeBranch:
		return Spec{Name: value, Type: TypeBranch}, nil
	case TypeTag:
		return Spec{Name: value, Type: TypeTag, ReadOnly: true}, nil
	case TypeHash:
		return Spec{Name: value, Type: TypeHash, Hash: value, ReadOnly: true}, nil
	case TypeSnapshot:
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return Spec{}, api.WrapError(api.ErrConfigInvalid, "Iceberg snapshot ref requires a numeric snapshot id", map[string]string{"ref": value}, err)
		}
		return Spec{Name: value, Type: TypeSnapshot, SnapshotID: id, ReadOnly: true}, nil
	default:
		return Spec{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg ref type is unsupported", map[string]string{"type": typ})
	}
}
