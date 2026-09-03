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

package arrowio

import (
	"context"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external/arrowio/ipcflatbuf"
)

const (
	// Keep the total field limit aligned with plan.TableColumnCountLimit. It
	// applies to top-level and nested fields together so a deeply nested schema
	// cannot multiply Arrow-Go's field allocations behind the table limit.
	maxArrowSchemaFields          = 4096
	maxArrowSchemaDepth           = 64
	maxArrowSchemaMetadataEntries = 4096
	maxArrowSchemaFeatures        = 64
	maxArrowUnionTypeIDsPerField  = 128
	maxArrowSchemaUnionTypeIDs    = 4096
)

type schemaMetadataBudget struct {
	metadataBytes  int
	fields         int
	metadata       int
	unionTypeIDs   int
	decodedStrings int
}

// validateIPCSchemaMetadata bounds every schema vector and recursively walks
// it before Arrow-Go's schemaFromFB can allocate slices from untrusted vector
// lengths. Walking every element also proves that the declared vector and
// string ranges fit in the metadata buffer. decodedStrings prevents aliased
// FlatBuffers offsets from amplifying a bounded wire message into unbounded Go
// string allocations.
func validateIPCSchemaMetadata(
	ctx context.Context,
	schema *ipcflatbuf.Schema,
	metadataBytes int,
) (retErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC schema metadata: %v", recovered)
		}
	}()
	if schema == nil || metadataBytes < 4 {
		return moerr.NewInvalidInput(ctx, "Arrow IPC schema metadata is missing")
	}
	budget := schemaMetadataBudget{metadataBytes: metadataBytes}
	if err := budget.validateSchema(ctx, schema); err != nil {
		return err
	}
	return nil
}

func (b *schemaMetadataBudget) validateSchema(ctx context.Context, schema *ipcflatbuf.Schema) error {
	fieldCount := schema.FieldsLength()
	if err := b.consumeVector(ctx, "field", fieldCount, 4); err != nil {
		return err
	}
	if err := b.consumeFields(ctx, fieldCount); err != nil {
		return err
	}
	if err := b.validateCustomMetadata(
		ctx, "schema", schema.CustomMetadataLength(), schema.CustomMetadata,
	); err != nil {
		return err
	}

	featureCount := schema.FeaturesLength()
	if err := b.consumeVector(ctx, "feature", featureCount, 8); err != nil {
		return err
	}
	if featureCount > maxArrowSchemaFeatures {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema feature count %d exceeds limit %d",
			featureCount, maxArrowSchemaFeatures)
	}
	for index := 0; index < featureCount; index++ {
		_ = schema.Features(index)
	}

	for index := 0; index < fieldCount; index++ {
		var field ipcflatbuf.Field
		if !schema.Fields(&field, index) {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC schema field %d is missing", index)
		}
		if err := b.validateField(ctx, &field, 1); err != nil {
			return moerr.NewInvalidInputf(ctx, "invalid Arrow IPC schema field %d: %v", index, err)
		}
	}
	return nil
}

func (b *schemaMetadataBudget) validateField(
	ctx context.Context,
	field *ipcflatbuf.Field,
	depth int,
) error {
	if depth > maxArrowSchemaDepth {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema nesting depth %d exceeds limit %d", depth, maxArrowSchemaDepth)
	}
	if err := b.consumeStringBytes(ctx, len(field.Name())); err != nil {
		return err
	}
	if err := b.validateCustomMetadata(
		ctx, "field", field.CustomMetadataLength(), field.CustomMetadata,
	); err != nil {
		return err
	}

	childCount := field.ChildrenLength()
	if err := b.consumeVector(ctx, "child field", childCount, 4); err != nil {
		return err
	}
	if err := b.consumeFields(ctx, childCount); err != nil {
		return err
	}
	for index := 0; index < childCount; index++ {
		var child ipcflatbuf.Field
		if !field.Children(&child, index) {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC schema child field %d is missing", index)
		}
		if err := b.validateField(ctx, &child, depth+1); err != nil {
			return moerr.NewInvalidInputf(ctx, "invalid Arrow IPC schema child field %d: %v", index, err)
		}
	}

	typeID := field.TypeType()
	var typeTable flatbuffers.Table
	if typeID == ipcflatbuf.TypeNone || !field.Type(&typeTable) {
		return moerr.NewInvalidInput(ctx, "Arrow IPC schema field type is missing")
	}
	if typeID == ipcflatbuf.TypeTimestamp {
		var timestamp ipcflatbuf.Timestamp
		timestamp.Init(typeTable.Bytes, typeTable.Pos)
		if err := b.consumeStringBytes(ctx, len(timestamp.Timezone())); err != nil {
			return err
		}
	}
	if typeID != ipcflatbuf.TypeUnion {
		return nil
	}
	if childCount > maxArrowUnionTypeIDsPerField {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC union child count %d exceeds limit %d",
			childCount, maxArrowUnionTypeIDsPerField)
	}
	var union ipcflatbuf.Union
	union.Init(typeTable.Bytes, typeTable.Pos)
	typeIDCount := union.TypeIDsLength()
	if err := b.consumeVector(ctx, "union type ID", typeIDCount, 4); err != nil {
		return err
	}
	if typeIDCount > maxArrowUnionTypeIDsPerField {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC union type ID count %d exceeds per-field limit %d",
			typeIDCount, maxArrowUnionTypeIDsPerField)
	}
	if typeIDCount > maxArrowSchemaUnionTypeIDs-b.unionTypeIDs {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC union type ID count exceeds limit %d", maxArrowSchemaUnionTypeIDs)
	}
	if typeIDCount != 0 && typeIDCount != childCount {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC union type ID count %d does not match child count %d",
			typeIDCount, childCount)
	}
	b.unionTypeIDs += typeIDCount
	for index := 0; index < typeIDCount; index++ {
		value := union.TypeIDs(index)
		if value < 0 || value >= maxArrowUnionTypeIDsPerField {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC union type ID %d at index %d is out of bounds", value, index)
		}
	}
	return nil
}

func (b *schemaMetadataBudget) validateCustomMetadata(
	ctx context.Context,
	owner string,
	count int,
	read func(*ipcflatbuf.KeyValue, int) bool,
) error {
	if err := b.consumeVector(ctx, owner+" custom-metadata", count, 4); err != nil {
		return err
	}
	if count > maxArrowSchemaMetadataEntries-b.metadata {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema custom metadata entry count exceeds limit %d",
			maxArrowSchemaMetadataEntries)
	}
	b.metadata += count
	for index := 0; index < count; index++ {
		var metadata ipcflatbuf.KeyValue
		if !read(&metadata, index) {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC %s custom metadata entry %d is missing", owner, index)
		}
		if err := b.consumeStringBytes(ctx, len(metadata.Key())); err != nil {
			return err
		}
		if err := b.consumeStringBytes(ctx, len(metadata.Value())); err != nil {
			return err
		}
	}
	return nil
}

func (b *schemaMetadataBudget) consumeFields(ctx context.Context, count int) error {
	if count < 0 || count > maxArrowSchemaFields-b.fields {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema field count exceeds limit %d", maxArrowSchemaFields)
	}
	b.fields += count
	return nil
}

func (b *schemaMetadataBudget) consumeStringBytes(ctx context.Context, count int) error {
	if count < 0 || count > b.metadataBytes-b.decodedStrings {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema decoded string bytes exceed metadata size %d", b.metadataBytes)
	}
	b.decodedStrings += count
	return nil
}

func (b *schemaMetadataBudget) consumeVector(
	ctx context.Context,
	name string,
	count int,
	elementBytes int,
) error {
	if count < 0 || elementBytes <= 0 ||
		uint64(count) > uint64(b.metadataBytes)/uint64(elementBytes) {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC schema %s vector count %d exceeds metadata size %d",
			name, count, b.metadataBytes)
	}
	return nil
}
