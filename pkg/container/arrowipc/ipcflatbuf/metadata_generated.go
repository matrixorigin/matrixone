// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0.

// Code generated from Apache Arrow format/Message.fbs by flatc and reduced to
// the read-only metadata surface used by MatrixOne. DO NOT EDIT.

package ipcflatbuf

import flatbuffers "github.com/google/flatbuffers/go"

const (
	blockSize     = flatbuffers.UOffsetT(24)
	fieldNodeSize = flatbuffers.UOffsetT(16)
	bufferSize    = flatbuffers.UOffsetT(16)
)

type MessageHeader byte

const (
	MessageHeaderDictionaryBatch MessageHeader = 2
	MessageHeaderRecordBatch     MessageHeader = 3
	MessageHeaderSchema          MessageHeader = 1
)

type Type byte

const (
	TypeNone      Type = 0
	TypeTimestamp Type = 10
	TypeUnion     Type = 14
)

type CompressionType int8

const (
	CompressionTypeLZ4Frame CompressionType = 0
	CompressionTypeZSTD     CompressionType = 1
)

type BodyCompressionMethod int8

const (
	BodyCompressionMethodBuffer BodyCompressionMethod = 0
)

type Block struct {
	tab flatbuffers.Struct
}

func (b *Block) Init(buf []byte, position flatbuffers.UOffsetT) {
	b.tab.Bytes = buf
	b.tab.Pos = position
}

func (b *Block) Offset() int64 {
	return b.tab.GetInt64(b.tab.Pos)
}

func (b *Block) MetadataLength() int32 {
	return b.tab.GetInt32(b.tab.Pos + 8)
}

func (b *Block) BodyLength() int64 {
	return b.tab.GetInt64(b.tab.Pos + 16)
}

type Footer struct {
	tab flatbuffers.Table
}

func GetRootAsFooter(buf []byte) *Footer {
	root := flatbuffers.GetUOffsetT(buf)
	footer := new(Footer)
	footer.Init(buf, root)
	return footer
}

func (f *Footer) Init(buf []byte, position flatbuffers.UOffsetT) {
	f.tab.Bytes = buf
	f.tab.Pos = position
}

func (f *Footer) Schema(schema *Schema) *Schema {
	offset := flatbuffers.UOffsetT(f.tab.Offset(6))
	if offset == 0 {
		return nil
	}
	position := f.tab.Indirect(offset + f.tab.Pos)
	if schema == nil {
		schema = new(Schema)
	}
	schema.Init(f.tab.Bytes, position)
	return schema
}

func (f *Footer) Dictionaries(block *Block, index int) bool {
	offset := flatbuffers.UOffsetT(f.tab.Offset(8))
	if offset == 0 {
		return false
	}
	position := f.tab.Vector(offset) + flatbuffers.UOffsetT(index)*blockSize
	block.Init(f.tab.Bytes, position)
	return true
}

func (f *Footer) DictionariesLength() int {
	offset := flatbuffers.UOffsetT(f.tab.Offset(8))
	if offset == 0 {
		return 0
	}
	return f.tab.VectorLen(offset)
}

func (f *Footer) RecordBatches(block *Block, index int) bool {
	offset := flatbuffers.UOffsetT(f.tab.Offset(10))
	if offset == 0 {
		return false
	}
	position := f.tab.Vector(offset) + flatbuffers.UOffsetT(index)*blockSize
	block.Init(f.tab.Bytes, position)
	return true
}

func (f *Footer) RecordBatchesLength() int {
	offset := flatbuffers.UOffsetT(f.tab.Offset(10))
	if offset == 0 {
		return 0
	}
	return f.tab.VectorLen(offset)
}

type Message struct {
	tab flatbuffers.Table
}

func GetRootAsMessage(buf []byte) *Message {
	root := flatbuffers.GetUOffsetT(buf)
	message := new(Message)
	message.Init(buf, root)
	return message
}

func (m *Message) Init(buf []byte, position flatbuffers.UOffsetT) {
	m.tab.Bytes = buf
	m.tab.Pos = position
}

func (m *Message) HeaderType() MessageHeader {
	offset := flatbuffers.UOffsetT(m.tab.Offset(6))
	if offset == 0 {
		return 0
	}
	return MessageHeader(m.tab.GetByte(offset + m.tab.Pos))
}

func (m *Message) BodyLength() int64 {
	offset := flatbuffers.UOffsetT(m.tab.Offset(10))
	if offset == 0 {
		return 0
	}
	return m.tab.GetInt64(offset + m.tab.Pos)
}

func (m *Message) RecordBatch(batch *RecordBatch) bool {
	if m.HeaderType() != MessageHeaderRecordBatch {
		return false
	}
	return m.header(func(table flatbuffers.Table) { batch.Init(table.Bytes, table.Pos) })
}

func (m *Message) Schema(schema *Schema) bool {
	if m.HeaderType() != MessageHeaderSchema {
		return false
	}
	return m.header(func(table flatbuffers.Table) { schema.Init(table.Bytes, table.Pos) })
}

func (m *Message) DictionaryBatch(batch *DictionaryBatch) bool {
	if m.HeaderType() != MessageHeaderDictionaryBatch {
		return false
	}
	return m.header(func(table flatbuffers.Table) { batch.Init(table.Bytes, table.Pos) })
}

func (m *Message) header(install func(flatbuffers.Table)) bool {
	offset := flatbuffers.UOffsetT(m.tab.Offset(8))
	if offset == 0 {
		return false
	}
	var table flatbuffers.Table
	m.tab.Union(&table, offset)
	install(table)
	return true
}

type Schema struct {
	tab flatbuffers.Table
}

func (s *Schema) Init(buf []byte, position flatbuffers.UOffsetT) {
	s.tab.Bytes = buf
	s.tab.Pos = position
}

func (s *Schema) Fields(field *Field, index int) bool {
	offset := flatbuffers.UOffsetT(s.tab.Offset(6))
	if offset == 0 {
		return false
	}
	position := s.tab.Vector(offset) + flatbuffers.UOffsetT(index)*4
	position = s.tab.Indirect(position)
	field.Init(s.tab.Bytes, position)
	return true
}

func (s *Schema) FieldsLength() int {
	offset := flatbuffers.UOffsetT(s.tab.Offset(6))
	if offset == 0 {
		return 0
	}
	return s.tab.VectorLen(offset)
}

func (s *Schema) CustomMetadata(metadata *KeyValue, index int) bool {
	offset := flatbuffers.UOffsetT(s.tab.Offset(8))
	if offset == 0 {
		return false
	}
	position := s.tab.Vector(offset) + flatbuffers.UOffsetT(index)*4
	position = s.tab.Indirect(position)
	metadata.Init(s.tab.Bytes, position)
	return true
}

func (s *Schema) CustomMetadataLength() int {
	offset := flatbuffers.UOffsetT(s.tab.Offset(8))
	if offset == 0 {
		return 0
	}
	return s.tab.VectorLen(offset)
}

func (s *Schema) Features(index int) int64 {
	offset := flatbuffers.UOffsetT(s.tab.Offset(10))
	if offset == 0 {
		return 0
	}
	position := s.tab.Vector(offset) + flatbuffers.UOffsetT(index)*8
	return s.tab.GetInt64(position)
}

func (s *Schema) FeaturesLength() int {
	offset := flatbuffers.UOffsetT(s.tab.Offset(10))
	if offset == 0 {
		return 0
	}
	return s.tab.VectorLen(offset)
}

type Field struct {
	tab flatbuffers.Table
}

func (f *Field) Init(buf []byte, position flatbuffers.UOffsetT) {
	f.tab.Bytes = buf
	f.tab.Pos = position
}

func (f *Field) Name() []byte {
	offset := flatbuffers.UOffsetT(f.tab.Offset(4))
	if offset == 0 {
		return nil
	}
	return f.tab.ByteVector(offset + f.tab.Pos)
}

func (f *Field) TypeType() Type {
	offset := flatbuffers.UOffsetT(f.tab.Offset(8))
	if offset == 0 {
		return TypeNone
	}
	return Type(f.tab.GetByte(offset + f.tab.Pos))
}

func (f *Field) Type(table *flatbuffers.Table) bool {
	offset := flatbuffers.UOffsetT(f.tab.Offset(10))
	if offset == 0 {
		return false
	}
	f.tab.Union(table, offset)
	return true
}

func (f *Field) Children(child *Field, index int) bool {
	offset := flatbuffers.UOffsetT(f.tab.Offset(14))
	if offset == 0 {
		return false
	}
	position := f.tab.Vector(offset) + flatbuffers.UOffsetT(index)*4
	position = f.tab.Indirect(position)
	child.Init(f.tab.Bytes, position)
	return true
}

func (f *Field) ChildrenLength() int {
	offset := flatbuffers.UOffsetT(f.tab.Offset(14))
	if offset == 0 {
		return 0
	}
	return f.tab.VectorLen(offset)
}

func (f *Field) CustomMetadata(metadata *KeyValue, index int) bool {
	offset := flatbuffers.UOffsetT(f.tab.Offset(16))
	if offset == 0 {
		return false
	}
	position := f.tab.Vector(offset) + flatbuffers.UOffsetT(index)*4
	position = f.tab.Indirect(position)
	metadata.Init(f.tab.Bytes, position)
	return true
}

func (f *Field) CustomMetadataLength() int {
	offset := flatbuffers.UOffsetT(f.tab.Offset(16))
	if offset == 0 {
		return 0
	}
	return f.tab.VectorLen(offset)
}

type KeyValue struct {
	tab flatbuffers.Table
}

func (k *KeyValue) Init(buf []byte, position flatbuffers.UOffsetT) {
	k.tab.Bytes = buf
	k.tab.Pos = position
}

func (k *KeyValue) Key() []byte {
	offset := flatbuffers.UOffsetT(k.tab.Offset(4))
	if offset == 0 {
		return nil
	}
	return k.tab.ByteVector(offset + k.tab.Pos)
}

func (k *KeyValue) Value() []byte {
	offset := flatbuffers.UOffsetT(k.tab.Offset(6))
	if offset == 0 {
		return nil
	}
	return k.tab.ByteVector(offset + k.tab.Pos)
}

type Union struct {
	tab flatbuffers.Table
}

type Timestamp struct {
	tab flatbuffers.Table
}

func (t *Timestamp) Init(buf []byte, position flatbuffers.UOffsetT) {
	t.tab.Bytes = buf
	t.tab.Pos = position
}

func (t *Timestamp) Timezone() []byte {
	offset := flatbuffers.UOffsetT(t.tab.Offset(6))
	if offset == 0 {
		return nil
	}
	return t.tab.ByteVector(offset + t.tab.Pos)
}

func (u *Union) Init(buf []byte, position flatbuffers.UOffsetT) {
	u.tab.Bytes = buf
	u.tab.Pos = position
}

func (u *Union) TypeIDs(index int) int32 {
	offset := flatbuffers.UOffsetT(u.tab.Offset(6))
	if offset == 0 {
		return 0
	}
	position := u.tab.Vector(offset) + flatbuffers.UOffsetT(index)*4
	return u.tab.GetInt32(position)
}

func (u *Union) TypeIDsLength() int {
	offset := flatbuffers.UOffsetT(u.tab.Offset(6))
	if offset == 0 {
		return 0
	}
	return u.tab.VectorLen(offset)
}

type DictionaryBatch struct {
	tab flatbuffers.Table
}

func (d *DictionaryBatch) Init(buf []byte, position flatbuffers.UOffsetT) {
	d.tab.Bytes = buf
	d.tab.Pos = position
}

func (d *DictionaryBatch) ID() int64 {
	offset := flatbuffers.UOffsetT(d.tab.Offset(4))
	if offset == 0 {
		return 0
	}
	return d.tab.GetInt64(offset + d.tab.Pos)
}

func (d *DictionaryBatch) Data(batch *RecordBatch) bool {
	offset := flatbuffers.UOffsetT(d.tab.Offset(6))
	if offset == 0 {
		return false
	}
	position := d.tab.Indirect(offset + d.tab.Pos)
	batch.Init(d.tab.Bytes, position)
	return true
}

func (d *DictionaryBatch) IsDelta() bool {
	offset := flatbuffers.UOffsetT(d.tab.Offset(8))
	return offset != 0 && d.tab.GetBool(offset+d.tab.Pos)
}

type RecordBatch struct {
	tab flatbuffers.Table
}

func (r *RecordBatch) Init(buf []byte, position flatbuffers.UOffsetT) {
	r.tab.Bytes = buf
	r.tab.Pos = position
}

func (r *RecordBatch) Length() int64 {
	offset := flatbuffers.UOffsetT(r.tab.Offset(4))
	if offset == 0 {
		return 0
	}
	return r.tab.GetInt64(offset + r.tab.Pos)
}

func (r *RecordBatch) Nodes(node *FieldNode, index int) bool {
	offset := flatbuffers.UOffsetT(r.tab.Offset(6))
	if offset == 0 {
		return false
	}
	position := r.tab.Vector(offset) + flatbuffers.UOffsetT(index)*fieldNodeSize
	node.Init(r.tab.Bytes, position)
	return true
}

func (r *RecordBatch) NodesLength() int {
	offset := flatbuffers.UOffsetT(r.tab.Offset(6))
	if offset == 0 {
		return 0
	}
	return r.tab.VectorLen(offset)
}

func (r *RecordBatch) Buffers(buffer *Buffer, index int) bool {
	offset := flatbuffers.UOffsetT(r.tab.Offset(8))
	if offset == 0 {
		return false
	}
	position := r.tab.Vector(offset) + flatbuffers.UOffsetT(index)*bufferSize
	buffer.Init(r.tab.Bytes, position)
	return true
}

func (r *RecordBatch) BuffersLength() int {
	offset := flatbuffers.UOffsetT(r.tab.Offset(8))
	if offset == 0 {
		return 0
	}
	return r.tab.VectorLen(offset)
}

func (r *RecordBatch) Compression(compression *BodyCompression) *BodyCompression {
	offset := flatbuffers.UOffsetT(r.tab.Offset(10))
	if offset == 0 {
		return nil
	}
	position := r.tab.Indirect(offset + r.tab.Pos)
	if compression == nil {
		compression = new(BodyCompression)
	}
	compression.Init(r.tab.Bytes, position)
	return compression
}

func (r *RecordBatch) VariadicBufferCounts(index int) int64 {
	offset := flatbuffers.UOffsetT(r.tab.Offset(12))
	if offset == 0 {
		return 0
	}
	position := r.tab.Vector(offset) + flatbuffers.UOffsetT(index)*8
	return r.tab.GetInt64(position)
}

func (r *RecordBatch) VariadicBufferCountsLength() int {
	offset := flatbuffers.UOffsetT(r.tab.Offset(12))
	if offset == 0 {
		return 0
	}
	return r.tab.VectorLen(offset)
}

type BodyCompression struct {
	tab flatbuffers.Table
}

func (c *BodyCompression) Init(buf []byte, position flatbuffers.UOffsetT) {
	c.tab.Bytes = buf
	c.tab.Pos = position
}

func (c *BodyCompression) Codec() CompressionType {
	offset := flatbuffers.UOffsetT(c.tab.Offset(4))
	if offset == 0 {
		return CompressionTypeLZ4Frame
	}
	return CompressionType(c.tab.GetInt8(offset + c.tab.Pos))
}

func (c *BodyCompression) Method() BodyCompressionMethod {
	offset := flatbuffers.UOffsetT(c.tab.Offset(6))
	if offset == 0 {
		return BodyCompressionMethodBuffer
	}
	return BodyCompressionMethod(c.tab.GetInt8(offset + c.tab.Pos))
}

type FieldNode struct {
	tab flatbuffers.Struct
}

func (n *FieldNode) Init(buf []byte, position flatbuffers.UOffsetT) {
	n.tab.Bytes = buf
	n.tab.Pos = position
}

func (n *FieldNode) Length() int64 {
	return n.tab.GetInt64(n.tab.Pos)
}

func (n *FieldNode) NullCount() int64 {
	return n.tab.GetInt64(n.tab.Pos + 8)
}

type Buffer struct {
	tab flatbuffers.Struct
}

func (b *Buffer) Init(buf []byte, position flatbuffers.UOffsetT) {
	b.tab.Bytes = buf
	b.tab.Pos = position
}

func (b *Buffer) Offset() int64 {
	return b.tab.GetInt64(b.tab.Pos)
}

func (b *Buffer) Length() int64 {
	return b.tab.GetInt64(b.tab.Pos + 8)
}
