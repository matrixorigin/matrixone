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

package api

import (
	"encoding/json"
	"strconv"
	"strings"
)

const MaxMODecimalPrecision = 38

type TableMetadata struct {
	FormatVersion        int                `json:"format-version"`
	TableUUID            string             `json:"table-uuid,omitempty"`
	Location             string             `json:"location"`
	LastSequenceNumber   int64              `json:"last-sequence-number,omitempty"`
	LastUpdatedMS        int64              `json:"last-updated-ms,omitempty"`
	CurrentSchemaID      int                `json:"current-schema-id"`
	Schemas              []Schema           `json:"schemas"`
	DefaultSpecID        int                `json:"default-spec-id"`
	PartitionSpecs       []PartitionSpec    `json:"partition-specs"`
	LastPartitionID      int                `json:"last-partition-id,omitempty"`
	CurrentSnapshotID    *int64             `json:"current-snapshot-id,omitempty"`
	Snapshots            []Snapshot         `json:"snapshots,omitempty"`
	SnapshotLog          []SnapshotLogEntry `json:"snapshot-log,omitempty"`
	MetadataLog          []MetadataLogEntry `json:"metadata-log,omitempty"`
	Refs                 map[string]SnapshotRef
	Properties           map[string]string `json:"properties,omitempty"`
	MetadataLocation     string            `json:"-"`
	MetadataLocationHash string            `json:"-"`
	MetadataLocationRed  string            `json:"-"`
	RawJSON              json.RawMessage   `json:"-"`
}

func (m *TableMetadata) CurrentSchema() (Schema, bool) {
	for _, schema := range m.Schemas {
		if schema.SchemaID == m.CurrentSchemaID {
			return schema, true
		}
	}
	return Schema{}, false
}

func (m *TableMetadata) DefaultSpec() (PartitionSpec, bool) {
	for _, spec := range m.PartitionSpecs {
		if spec.SpecID == m.DefaultSpecID {
			return spec, true
		}
	}
	return PartitionSpec{}, false
}

type SnapshotRef struct {
	Name               string `json:"-"`
	SnapshotID         int64  `json:"snapshot-id"`
	Type               string `json:"type"`
	MinSnapshotsToKeep int    `json:"min-snapshots-to-keep,omitempty"`
	MaxSnapshotAgeMS   int64  `json:"max-snapshot-age-ms,omitempty"`
	MaxRefAgeMS        int64  `json:"max-ref-age-ms,omitempty"`
}

type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number,omitempty"`
	TimestampMS      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list,omitempty"`
	SchemaID         *int              `json:"schema-id,omitempty"`
	Summary          map[string]string `json:"summary,omitempty"`
}

type SnapshotLogEntry struct {
	TimestampMS int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

type MetadataLogEntry struct {
	TimestampMS  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

type Schema struct {
	SchemaID           int           `json:"schema-id"`
	Fields             []SchemaField `json:"fields"`
	IdentifierFieldIDs []int         `json:"identifier-field-ids,omitempty"`
}

type SchemaField struct {
	ID             int             `json:"id"`
	Name           string          `json:"name"`
	Required       bool            `json:"required"`
	Type           IcebergType     `json:"type"`
	Doc            string          `json:"doc,omitempty"`
	InitialDefault json.RawMessage `json:"initial-default,omitempty"`
	WriteDefault   json.RawMessage `json:"write-default,omitempty"`
}

type IcebergTypeKind string

const (
	TypeUnknown     IcebergTypeKind = "unknown"
	TypeBoolean     IcebergTypeKind = "boolean"
	TypeInt         IcebergTypeKind = "int"
	TypeLong        IcebergTypeKind = "long"
	TypeFloat       IcebergTypeKind = "float"
	TypeDouble      IcebergTypeKind = "double"
	TypeDecimal     IcebergTypeKind = "decimal"
	TypeDate        IcebergTypeKind = "date"
	TypeTime        IcebergTypeKind = "time"
	TypeTimestamp   IcebergTypeKind = "timestamp"
	TypeTimestampTZ IcebergTypeKind = "timestamptz"
	TypeString      IcebergTypeKind = "string"
	TypeBinary      IcebergTypeKind = "binary"
	TypeFixed       IcebergTypeKind = "fixed"
	TypeUUID        IcebergTypeKind = "uuid"
	TypeStruct      IcebergTypeKind = "struct"
	TypeList        IcebergTypeKind = "list"
	TypeMap         IcebergTypeKind = "map"
	TypeVariant     IcebergTypeKind = "variant"
	TypeGeometry    IcebergTypeKind = "geometry"
	TypeGeography   IcebergTypeKind = "geography"
	TypeTimestampNS IcebergTypeKind = "timestamp_ns"
)

type IcebergType struct {
	Kind            IcebergTypeKind
	Raw             string
	Precision       int
	Scale           int
	Length          int
	Fields          []SchemaField
	ElementID       int
	Element         *IcebergType
	ElementRequired bool
	KeyID           int
	Key             *IcebergType
	ValueID         int
	Value           *IcebergType
	ValueRequired   bool
}

func (t IcebergType) String() string {
	if t.Raw != "" {
		return t.Raw
	}
	switch t.Kind {
	case TypeDecimal:
		return "decimal(" + strconv.Itoa(t.Precision) + "," + strconv.Itoa(t.Scale) + ")"
	case TypeFixed:
		return "fixed[" + strconv.Itoa(t.Length) + "]"
	default:
		if t.Kind == "" {
			return string(TypeUnknown)
		}
		return string(t.Kind)
	}
}

func (t IcebergType) MarshalJSON() ([]byte, error) {
	switch t.Kind {
	case TypeStruct:
		return json.Marshal(struct {
			Type   string        `json:"type"`
			Fields []SchemaField `json:"fields"`
		}{Type: string(TypeStruct), Fields: t.Fields})
	case TypeList:
		if t.Element == nil {
			return nil, NewError(ErrMetadataInvalid, "Iceberg list type is missing element type", nil)
		}
		return json.Marshal(struct {
			Type            string      `json:"type"`
			ElementID       int         `json:"element-id"`
			Element         IcebergType `json:"element"`
			ElementRequired bool        `json:"element-required"`
		}{
			Type:            string(TypeList),
			ElementID:       t.ElementID,
			Element:         *t.Element,
			ElementRequired: t.ElementRequired,
		})
	case TypeMap:
		if t.Key == nil || t.Value == nil {
			return nil, NewError(ErrMetadataInvalid, "Iceberg map type is missing key or value type", nil)
		}
		return json.Marshal(struct {
			Type          string      `json:"type"`
			KeyID         int         `json:"key-id"`
			Key           IcebergType `json:"key"`
			ValueID       int         `json:"value-id"`
			Value         IcebergType `json:"value"`
			ValueRequired bool        `json:"value-required"`
		}{
			Type:          string(TypeMap),
			KeyID:         t.KeyID,
			Key:           *t.Key,
			ValueID:       t.ValueID,
			Value:         *t.Value,
			ValueRequired: t.ValueRequired,
		})
	default:
		return json.Marshal(t.String())
	}
}

func (t *IcebergType) UnmarshalJSON(data []byte) error {
	var primitive string
	if err := json.Unmarshal(data, &primitive); err == nil {
		parsed, parseErr := ParseIcebergTypeString(primitive)
		if parseErr != nil {
			return parseErr
		}
		*t = parsed
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return WrapError(ErrMetadataInvalid, "Iceberg type JSON is invalid", nil, err)
	}
	var typeName string
	if err := json.Unmarshal(obj["type"], &typeName); err != nil || strings.TrimSpace(typeName) == "" {
		return WrapError(ErrMetadataInvalid, "Iceberg type object is missing type", nil, err)
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case string(TypeStruct):
		var fields []SchemaField
		if err := json.Unmarshal(obj["fields"], &fields); err != nil {
			return WrapError(ErrMetadataInvalid, "Iceberg struct fields are invalid", nil, err)
		}
		*t = IcebergType{Kind: TypeStruct, Raw: string(TypeStruct), Fields: fields}
	case string(TypeList):
		element, err := parseNestedType(obj["element"])
		if err != nil {
			return err
		}
		*t = IcebergType{Kind: TypeList, Raw: string(TypeList), Element: &element}
		_ = json.Unmarshal(obj["element-id"], &t.ElementID)
		_ = json.Unmarshal(obj["element-required"], &t.ElementRequired)
	case string(TypeMap):
		key, err := parseNestedType(obj["key"])
		if err != nil {
			return err
		}
		value, err := parseNestedType(obj["value"])
		if err != nil {
			return err
		}
		*t = IcebergType{Kind: TypeMap, Raw: string(TypeMap), Key: &key, Value: &value}
		_ = json.Unmarshal(obj["key-id"], &t.KeyID)
		_ = json.Unmarshal(obj["value-id"], &t.ValueID)
		_ = json.Unmarshal(obj["value-required"], &t.ValueRequired)
	case string(TypeFixed):
		*t = IcebergType{Kind: TypeFixed}
		_ = json.Unmarshal(obj["length"], &t.Length)
	default:
		parsed, parseErr := ParseIcebergTypeString(typeName)
		if parseErr != nil {
			return parseErr
		}
		*t = parsed
	}
	return nil
}

func parseNestedType(data json.RawMessage) (IcebergType, error) {
	if len(data) == 0 {
		return IcebergType{}, NewError(ErrMetadataInvalid, "Iceberg nested type is missing", nil)
	}
	var typ IcebergType
	if err := json.Unmarshal(data, &typ); err != nil {
		return IcebergType{}, err
	}
	return typ, nil
}

func ParseIcebergTypeString(raw string) (IcebergType, error) {
	raw = strings.TrimSpace(raw)
	lower := strings.ToLower(raw)
	switch lower {
	case string(TypeBoolean), string(TypeInt), string(TypeLong), string(TypeFloat), string(TypeDouble),
		string(TypeDate), string(TypeTime), string(TypeTimestamp), string(TypeTimestampTZ), string(TypeString),
		string(TypeBinary), string(TypeUUID), string(TypeVariant), string(TypeGeometry), string(TypeGeography):
		return IcebergType{Kind: IcebergTypeKind(lower), Raw: lower}, nil
	case "timestamp_ns", "timestamp-ns", "timestamp_ns_tz", "timestamptz_ns", "timestamptz-ns":
		return IcebergType{Kind: TypeTimestampNS, Raw: lower}, nil
	}
	if strings.HasPrefix(lower, "decimal(") && strings.HasSuffix(lower, ")") {
		body := strings.TrimSuffix(strings.TrimPrefix(lower, "decimal("), ")")
		parts := strings.Split(body, ",")
		if len(parts) != 2 {
			return IcebergType{}, NewError(ErrMetadataInvalid, "Iceberg decimal type is invalid", map[string]string{"type": raw})
		}
		precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return IcebergType{}, WrapError(ErrMetadataInvalid, "Iceberg decimal precision is invalid", map[string]string{"type": raw}, err)
		}
		scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return IcebergType{}, WrapError(ErrMetadataInvalid, "Iceberg decimal scale is invalid", map[string]string{"type": raw}, err)
		}
		return IcebergType{Kind: TypeDecimal, Raw: "decimal(" + strconv.Itoa(precision) + "," + strconv.Itoa(scale) + ")", Precision: precision, Scale: scale}, nil
	}
	if strings.HasPrefix(lower, "fixed[") && strings.HasSuffix(lower, "]") {
		body := strings.TrimSuffix(strings.TrimPrefix(lower, "fixed["), "]")
		length, err := strconv.Atoi(strings.TrimSpace(body))
		if err != nil {
			return IcebergType{}, WrapError(ErrMetadataInvalid, "Iceberg fixed length is invalid", map[string]string{"type": raw}, err)
		}
		return IcebergType{Kind: TypeFixed, Raw: "fixed[" + strconv.Itoa(length) + "]", Length: length}, nil
	}
	return IcebergType{Kind: TypeUnknown, Raw: raw}, NewError(ErrUnsupportedFeature, "Iceberg type is unsupported", map[string]string{"type": raw})
}

type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

type PartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type ManifestContent string

const (
	ManifestContentData    ManifestContent = "data"
	ManifestContentDeletes ManifestContent = "deletes"
)

type ManifestFile struct {
	Path                     string
	Length                   int64
	PartitionSpecID          int
	Content                  ManifestContent
	SequenceNumber           int64
	MinSequenceNumber        int64
	AddedSnapshotID          int64
	AddedFilesCount          int
	ExistingFilesCount       int
	DeletedFilesCount        int
	AddedRowsCount           int64
	ExistingRowsCount        int64
	DeletedRowsCount         int64
	Partitions               []PartitionFieldSummary
	KeyMetadata              []byte
	FirstRowID               *int64
	AddedFilesSizeInBytes    int64
	ExistingFilesSizeInBytes int64
	DeletedFilesSizeInBytes  int64
	ReferencedDataFilesCount int
	ManifestPathRedacted     string
	ManifestPathHash         string
}

type PartitionFieldSummary struct {
	ContainsNull bool
	ContainsNaN  bool
	LowerBound   []byte
	UpperBound   []byte
}

type ManifestEntryStatus int

const (
	ManifestEntryExisting ManifestEntryStatus = 0
	ManifestEntryAdded    ManifestEntryStatus = 1
	ManifestEntryDeleted  ManifestEntryStatus = 2
)

type ManifestEntry struct {
	Status         ManifestEntryStatus
	SnapshotID     int64
	SequenceNumber int64
	FileSequence   int64
	DataFile       DataFile
}

type DataFileContent int

const (
	DataFileContentData           DataFileContent = 0
	DataFileContentPositionDelete DataFileContent = 1
	DataFileContentEqualityDelete DataFileContent = 2
)

type DataFile struct {
	Content               DataFileContent
	FilePath              string
	FileFormat            string
	Partition             map[string]any
	PartitionFieldIDs     map[string]int
	RecordCount           int64
	FileSizeInBytes       int64
	ColumnSizes           map[int]int64
	ValueCounts           map[int]int64
	NullValueCounts       map[int]int64
	NaNValueCounts        map[int]int64
	LowerBounds           map[int][]byte
	UpperBounds           map[int][]byte
	SplitOffsets          []int64
	EqualityIDs           []int
	SortOrderID           int
	SpecID                int
	SequenceNumber        int64
	FileSequenceNumber    int64
	ReferencedDataFile    string
	DeleteSchemaID        int
	KeyMetadata           []byte
	FirstRowID            *int64
	EncryptionKeyMetadata []byte
	DeletionVectorPath    string
	FilePathRedacted      string
	FilePathHash          string
}

type MOType struct {
	Name      string
	Width     int
	Scale     int
	IcebergID int
}

func (t MOType) String() string {
	switch t.Name {
	case "DECIMAL":
		return "DECIMAL(" + strconv.Itoa(t.Width) + "," + strconv.Itoa(t.Scale) + ")"
	case "":
		return ""
	default:
		return t.Name
	}
}

type UnsupportedFeature struct {
	Feature string
	Reason  string
	Path    string
}
