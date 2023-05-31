// Copyright 2021 Matrix Origin
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

package tree

import (
	"fmt"
	"strconv"
	"strings"
)

type CreateOption interface {
	NodeFormatter
}

type createOptionImpl struct {
	CreateOption
}

type CreateOptionDefault struct {
	createOptionImpl
}

type CreateOptionCharset struct {
	createOptionImpl
	IsDefault bool
	Charset   string
}

func (node *CreateOptionCharset) Format(ctx *FmtCtx) {
	if node.IsDefault {
		ctx.WriteString("default ")
	}
	ctx.WriteString("character set ")
	ctx.WriteString(node.Charset)
}

func NewCreateOptionCharset(c string) *CreateOptionCharset {
	return &CreateOptionCharset{
		Charset: c,
	}
}

type CreateOptionCollate struct {
	createOptionImpl
	IsDefault bool
	Collate   string
}

func (node *CreateOptionCollate) Format(ctx *FmtCtx) {
	if node.IsDefault {
		ctx.WriteString("default ")
	}
	ctx.WriteString("collate ")
	ctx.WriteString(node.Collate)
}

func NewCreateOptionCollate(c string) *CreateOptionCollate {
	return &CreateOptionCollate{
		Collate: c,
	}
}

type CreateOptionEncryption struct {
	createOptionImpl
	Encrypt string
}

func (node *CreateOptionEncryption) Format(ctx *FmtCtx) {
	ctx.WriteString("encryption ")
	ctx.WriteString(node.Encrypt)
}

func NewCreateOptionEncryption(e string) *CreateOptionEncryption {
	return &CreateOptionEncryption{
		Encrypt: e,
	}
}

type SubscriptionOption struct {
	statementImpl
	From        Identifier
	Publication Identifier
}

func (node *SubscriptionOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" from ")
	node.From.Format(ctx)
	ctx.WriteString(" publication ")
	node.Publication.Format(ctx)
}

type CreateDatabase struct {
	statementImpl
	IfNotExists        bool
	Name               Identifier
	CreateOptions      []CreateOption
	SubscriptionOption *SubscriptionOption
}

func (node *CreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("create database ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)

	if node.SubscriptionOption != nil {
		node.SubscriptionOption.Format(ctx)
	}

	if node.CreateOptions != nil {
		for _, opt := range node.CreateOptions {
			ctx.WriteByte(' ')
			opt.Format(ctx)
		}
	}
}

func (node *CreateDatabase) GetStatementType() string { return "Create Database" }
func (node *CreateDatabase) GetQueryType() string     { return QueryTypeDDL }

func NewCreateDatabase(ine bool, name Identifier, opts []CreateOption) *CreateDatabase {
	return &CreateDatabase{
		IfNotExists:   ine,
		Name:          name,
		CreateOptions: opts,
	}
}

type CreateTable struct {
	statementImpl
	/*
		it is impossible to be the temporary table, the cluster table,
		the normal table and the external table at the same time.
	*/
	Temporary       bool
	IsClusterTable  bool
	IfNotExists     bool
	Table           TableName
	Defs            TableDefs
	Options         []TableOption
	PartitionOption *PartitionOption
	ClusterByOption *ClusterByOption
	Param           *ExternParam
}

func (node *CreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("create")
	if node.Temporary {
		ctx.WriteString(" temporary")
	}
	if node.IsClusterTable {
		ctx.WriteString(" cluster")
	}
	if node.Param != nil {
		ctx.WriteString(" external")
	}

	ctx.WriteString(" table")

	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}

	ctx.WriteByte(' ')
	node.Table.Format(ctx)

	ctx.WriteString(" (")
	for i, def := range node.Defs {
		if i != 0 {
			ctx.WriteString(",")
			ctx.WriteByte(' ')
		}
		def.Format(ctx)
	}
	ctx.WriteByte(')')

	if node.Options != nil {
		prefix := " "
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
			prefix = " "
		}
	}

	if node.PartitionOption != nil {
		ctx.WriteByte(' ')
		node.PartitionOption.Format(ctx)
	}

	if node.Param != nil {
		if len(node.Param.Option) == 0 {
			ctx.WriteString(" infile ")
			ctx.WriteString("'" + node.Param.Filepath + "'")
		} else {
			if node.Param.ScanType == S3 {
				ctx.WriteString(" url s3option ")
			} else {
				ctx.WriteString(" infile ")
			}
			ctx.WriteString("{")
			for i := 0; i < len(node.Param.Option); i += 2 {
				switch strings.ToLower(node.Param.Option[i]) {
				case "endpoint":
					ctx.WriteString("'endpoint'='" + node.Param.Option[i+1] + "'")
				case "region":
					ctx.WriteString("'region'='" + node.Param.Option[i+1] + "'")
				case "access_key_id":
					ctx.WriteString("'access_key_id'='******'")
				case "secret_access_key":
					ctx.WriteString("'secret_access_key'='******'")
				case "bucket":
					ctx.WriteString("'bucket'='" + node.Param.Option[i+1] + "'")
				case "filepath":
					ctx.WriteString("'filepath'='" + node.Param.Option[i+1] + "'")
				case "compression":
					ctx.WriteString("'compression'='" + node.Param.Option[i+1] + "'")
				case "format":
					ctx.WriteString("'format'='" + node.Param.Option[i+1] + "'")
				case "jsondata":
					ctx.WriteString("'jsondata'='" + node.Param.Option[i+1] + "'")
				}
				if i != len(node.Param.Option)-2 {
					ctx.WriteString(", ")
				}
			}
			ctx.WriteString("}")
		}
		if node.Param.Tail.Fields != nil {
			ctx.WriteByte(' ')
			node.Param.Tail.Fields.Format(ctx)
		}

		if node.Param.Tail.Lines != nil {
			ctx.WriteByte(' ')
			node.Param.Tail.Lines.Format(ctx)
		}

		if node.Param.Tail.IgnoredLines != 0 {
			ctx.WriteString(" ignore ")
			ctx.WriteString(strconv.FormatUint(node.Param.Tail.IgnoredLines, 10))
			ctx.WriteString(" lines")
		}
		if node.Param.Tail.ColumnList != nil {
			prefix := " ("
			for _, c := range node.Param.Tail.ColumnList {
				ctx.WriteString(prefix)
				c.Format(ctx)
				prefix = ", "
			}
			ctx.WriteByte(')')
		}
		if node.Param.Tail.Assignments != nil {
			ctx.WriteString(" set ")
			node.Param.Tail.Assignments.Format(ctx)
		}
	}
}

func (node *CreateTable) GetStatementType() string { return "Create Table" }
func (node *CreateTable) GetQueryType() string     { return QueryTypeDDL }

type TableDef interface {
	NodeFormatter
}

type tableDefImpl struct {
	TableDef
}

// the list of table definitions
type TableDefs []TableDef

type ColumnTableDef struct {
	tableDefImpl
	Name       *UnresolvedName
	Type       ResolvableTypeReference
	Attributes []ColumnAttribute
}

func (node *ColumnTableDef) Format(ctx *FmtCtx) {
	node.Name.Format(ctx)

	ctx.WriteByte(' ')
	node.Type.(*T).InternalType.Format(ctx)

	if node.Attributes != nil {
		prefix := " "
		for _, a := range node.Attributes {
			ctx.WriteString(prefix)
			a.Format(ctx)
		}
	}
}

func NewColumnTableDef(n *UnresolvedName, t ResolvableTypeReference, a []ColumnAttribute) *ColumnTableDef {
	return &ColumnTableDef{
		Name:       n,
		Type:       t,
		Attributes: a,
	}
}

// column attribute
type ColumnAttribute interface {
	NodeFormatter
}

type columnAttributeImpl struct {
	ColumnAttribute
}

type AttributeNull struct {
	columnAttributeImpl
	Is bool //true NULL (default); false NOT NULL
}

func (node *AttributeNull) Format(ctx *FmtCtx) {
	if node.Is {
		ctx.WriteString("null")
	} else {
		ctx.WriteString("not null")
	}
}

func NewAttributeNull(b bool) *AttributeNull {
	return &AttributeNull{
		Is: b,
	}
}

type AttributeDefault struct {
	columnAttributeImpl
	Expr Expr
}

func (node *AttributeDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("default ")
	node.Expr.Format(ctx)
}

func NewAttributeDefault(e Expr) *AttributeDefault {
	return &AttributeDefault{
		Expr: e,
	}
}

type AttributeAutoIncrement struct {
	columnAttributeImpl
	IsAutoIncrement bool
}

func (node *AttributeAutoIncrement) Format(ctx *FmtCtx) {
	ctx.WriteString("auto_increment")
}

func NewAttributeAutoIncrement() *AttributeAutoIncrement {
	return &AttributeAutoIncrement{}
}

type AttributeUniqueKey struct {
	columnAttributeImpl
}

func (node *AttributeUniqueKey) Format(ctx *FmtCtx) {
	ctx.WriteString("unique key")
}

func NewAttributeUniqueKey() *AttributeUniqueKey {
	return &AttributeUniqueKey{}
}

type AttributeUnique struct {
	columnAttributeImpl
}

func (node *AttributeUnique) Format(ctx *FmtCtx) {
	ctx.WriteString("unique")
}

func NewAttributeUnique() *AttributeUnique {
	return &AttributeUnique{}
}

type AttributeKey struct {
	columnAttributeImpl
}

func (node *AttributeKey) Format(ctx *FmtCtx) {
	ctx.WriteString("key")
}

func NewAttributeKey() *AttributeKey {
	return &AttributeKey{}
}

type AttributePrimaryKey struct {
	columnAttributeImpl
}

func (node *AttributePrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString("primary key")
}

func NewAttributePrimaryKey() *AttributePrimaryKey {
	return &AttributePrimaryKey{}
}

type AttributeComment struct {
	columnAttributeImpl
	CMT Expr
}

func (node *AttributeComment) Format(ctx *FmtCtx) {
	ctx.WriteString("comment ")
	node.CMT.Format(ctx)
}

func NewAttributeComment(c Expr) *AttributeComment {
	return &AttributeComment{
		CMT: c,
	}
}

type AttributeCollate struct {
	columnAttributeImpl
	Collate string
}

func (node *AttributeCollate) Format(ctx *FmtCtx) {
	ctx.WriteString("collate ")
	ctx.WriteString(node.Collate)
}

func NewAttributeCollate(c string) *AttributeCollate {
	return &AttributeCollate{
		Collate: c,
	}
}

type AttributeColumnFormat struct {
	columnAttributeImpl
	ColumnFormat string
}

func (node *AttributeColumnFormat) Format(ctx *FmtCtx) {
	ctx.WriteString("format ")
	ctx.WriteString(node.ColumnFormat)
}

func NewAttributeColumnFormat(f string) *AttributeColumnFormat {
	return &AttributeColumnFormat{
		ColumnFormat: f,
	}
}

type AttributeStorage struct {
	columnAttributeImpl
	Storage string
}

func (node *AttributeStorage) Format(ctx *FmtCtx) {
	ctx.WriteString("storage ")
	ctx.WriteString(node.Storage)
}

func NewAttributeStorage(s string) *AttributeStorage {
	return &AttributeStorage{
		Storage: s,
	}
}

type AttributeCheckConstraint struct {
	columnAttributeImpl
	Name     string
	Expr     Expr
	Enforced bool
}

func (node *AttributeCheckConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString("constraint")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	ctx.WriteString(" check")
	ctx.WriteString(" (")
	node.Expr.Format(ctx)
	ctx.WriteString(") ")

	if node.Enforced {
		ctx.WriteString("enforced")
	} else {
		ctx.WriteString("not enforced")
	}
}

func NewAttributeCheck(e Expr, f bool, n string) *AttributeCheckConstraint {
	return &AttributeCheckConstraint{
		Name:     n,
		Expr:     e,
		Enforced: f,
	}
}

type AttributeGeneratedAlways struct {
	columnAttributeImpl
	Expr   Expr
	Stored bool
}

func (node *AttributeGeneratedAlways) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
}

func NewAttributeGeneratedAlways(e Expr, s bool) *AttributeGeneratedAlways {
	return &AttributeGeneratedAlways{
		Expr:   e,
		Stored: s,
	}
}

type AttributeLowCardinality struct {
	columnAttributeImpl
}

func (node *AttributeLowCardinality) Format(ctx *FmtCtx) {
	ctx.WriteString("low_cardinality")
}

func NewAttributeLowCardinality() *AttributeLowCardinality {
	return &AttributeLowCardinality{}
}

type KeyPart struct {
	columnAttributeImpl
	ColName   *UnresolvedName
	Length    int
	Direction Direction // asc or desc
	Expr      Expr
}

func (node *KeyPart) Format(ctx *FmtCtx) {
	if node.ColName != nil {
		node.ColName.Format(ctx)
	}
	if node.Length != 0 {
		ctx.WriteByte('(')
		if node.Length == -1 {
			ctx.WriteString("0")
		} else {
			ctx.WriteString(strconv.Itoa(node.Length))
		}
		ctx.WriteByte(')')
	}
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
		return
	}
	if node.Expr != nil {
		ctx.WriteByte('(')
		node.Expr.Format(ctx)
		ctx.WriteByte(')')
		if node.Direction != DefaultDirection {
			ctx.WriteByte(' ')
			ctx.WriteString(node.Direction.String())
		}
	}
}

func NewKeyPart(c *UnresolvedName, l int, e Expr) *KeyPart {
	return &KeyPart{
		ColName: c,
		Length:  l,
		Expr:    e,
	}
}

// in reference definition
type MatchType int

func (node *MatchType) ToString() string {
	switch *node {
	case MATCH_FULL:
		return "full"
	case MATCH_PARTIAL:
		return "partial"
	case MATCH_SIMPLE:
		return "simple"
	default:
		return "Unknown MatchType"
	}
}

const (
	MATCH_INVALID MatchType = iota
	MATCH_FULL
	MATCH_PARTIAL
	MATCH_SIMPLE
)

type ReferenceOptionType int

func (node *ReferenceOptionType) ToString() string {
	switch *node {
	case REFERENCE_OPTION_RESTRICT:
		return "restrict"
	case REFERENCE_OPTION_CASCADE:
		return "cascade"
	case REFERENCE_OPTION_SET_NULL:
		return "set null"
	case REFERENCE_OPTION_NO_ACTION:
		return "no action"
	case REFERENCE_OPTION_SET_DEFAULT:
		return "set default"
	default:
		return "Unknown ReferenceOptionType"
	}
}

// Reference option
const (
	REFERENCE_OPTION_INVALID ReferenceOptionType = iota
	REFERENCE_OPTION_RESTRICT
	REFERENCE_OPTION_CASCADE
	REFERENCE_OPTION_SET_NULL
	REFERENCE_OPTION_NO_ACTION
	REFERENCE_OPTION_SET_DEFAULT
)

type AttributeReference struct {
	columnAttributeImpl
	TableName *TableName
	KeyParts  []*KeyPart
	Match     MatchType
	OnDelete  ReferenceOptionType
	OnUpdate  ReferenceOptionType
}

func (node *AttributeReference) Format(ctx *FmtCtx) {
	ctx.WriteString("references ")
	node.TableName.Format(ctx)
	if node.KeyParts != nil {
		ctx.WriteByte('(')
		prefix := ""
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Match != MATCH_INVALID {
		ctx.WriteString(" match ")
		ctx.WriteString(node.Match.ToString())
	}
	if node.OnDelete != REFERENCE_OPTION_INVALID {
		ctx.WriteString(" on delete ")
		ctx.WriteString(node.OnDelete.ToString())
	}
	if node.OnUpdate != REFERENCE_OPTION_INVALID {
		ctx.WriteString(" on update ")
		ctx.WriteString(node.OnUpdate.ToString())
	}
}

func NewAttributeReference(t *TableName, kps []*KeyPart, m MatchType,
	od ReferenceOptionType, ou ReferenceOptionType) *AttributeReference {
	return &AttributeReference{
		TableName: t,
		KeyParts:  kps,
		Match:     m,
		OnDelete:  od,
		OnUpdate:  ou,
	}
}

type ReferenceOnRecord struct {
	OnDelete ReferenceOptionType
	OnUpdate ReferenceOptionType
}

type AttributeAutoRandom struct {
	columnAttributeImpl
	BitLength int
}

func NewAttributeAutoRandom(b int) *AttributeAutoRandom {
	return &AttributeAutoRandom{
		BitLength: b,
	}
}

type AttributeOnUpdate struct {
	columnAttributeImpl
	Expr Expr
}

func (node *AttributeOnUpdate) Format(ctx *FmtCtx) {
	ctx.WriteString("on update ")
	node.Expr.Format(ctx)
}

func NewAttributeOnUpdate(e Expr) *AttributeOnUpdate {
	return &AttributeOnUpdate{
		Expr: e,
	}
}

type IndexType int

func (it IndexType) ToString() string {
	switch it {
	case INDEX_TYPE_BTREE:
		return "btree"
	case INDEX_TYPE_HASH:
		return "hash"
	case INDEX_TYPE_RTREE:
		return "rtree"
	case INDEX_TYPE_BSI:
		return "bsi"
	case INDEX_TYPE_ZONEMAP:
		return "zonemap"
	default:
		return "Unknown IndexType"
	}
}

const (
	INDEX_TYPE_INVALID IndexType = iota
	INDEX_TYPE_BTREE
	INDEX_TYPE_HASH
	INDEX_TYPE_RTREE
	INDEX_TYPE_BSI
	INDEX_TYPE_ZONEMAP
)

type VisibleType int

const (
	VISIBLE_TYPE_INVALID VisibleType = iota
	VISIBLE_TYPE_VISIBLE
	VISIBLE_TYPE_INVISIBLE
)

func (vt VisibleType) ToString() string {
	switch vt {
	case VISIBLE_TYPE_VISIBLE:
		return "visible"
	case VISIBLE_TYPE_INVISIBLE:
		return "invisible"
	default:
		return "Unknown VisibleType"
	}
}

type IndexOption struct {
	NodeFormatter
	KeyBlockSize             uint64
	IType                    IndexType
	ParserName               string
	Comment                  string
	Visible                  VisibleType
	EngineAttribute          string
	SecondaryEngineAttribute string
}

// Must follow the following sequence when test
func (node *IndexOption) Format(ctx *FmtCtx) {
	if node.KeyBlockSize != 0 {
		ctx.WriteString("KEY_BLOCK_SIZE ")
		ctx.WriteString(strconv.FormatUint(node.KeyBlockSize, 10))
		ctx.WriteByte(' ')
	}
	if node.ParserName != "" {
		ctx.WriteString("with parser ")
		ctx.WriteString(node.ParserName)
		ctx.WriteByte(' ')
	}
	if node.Comment != "" {
		ctx.WriteString("comment ")
		ctx.WriteString(node.Comment)
		ctx.WriteByte(' ')
	}
	if node.Visible != VISIBLE_TYPE_INVALID {
		ctx.WriteString(node.Visible.ToString())
	}
}

func NewIndexOption(k uint64, i IndexType, p string, c string, v VisibleType, e string, se string) *IndexOption {
	return &IndexOption{
		KeyBlockSize:             k,
		IType:                    i,
		ParserName:               p,
		Comment:                  c,
		Visible:                  v,
		EngineAttribute:          e,
		SecondaryEngineAttribute: se,
	}
}

type PrimaryKeyIndex struct {
	tableDefImpl
	KeyParts         []*KeyPart
	Name             string
	ConstraintSymbol string
	Empty            bool
	IndexOption      *IndexOption
}

func (node *PrimaryKeyIndex) Format(ctx *FmtCtx) {
	if node.ConstraintSymbol != "" {
		ctx.WriteString("constraint " + node.ConstraintSymbol + " ")
	}
	ctx.WriteString("primary key")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	if !node.Empty {
		ctx.WriteString(" using none")
	}
	if node.KeyParts != nil {
		prefix := " ("
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.IndexOption != nil {
		ctx.WriteByte(' ')
		node.IndexOption.Format(ctx)
	}
}

func NewPrimaryKeyIndex(k []*KeyPart, n string, e bool, io *IndexOption) *PrimaryKeyIndex {
	return &PrimaryKeyIndex{
		KeyParts:    k,
		Name:        n,
		Empty:       e,
		IndexOption: io,
	}
}

type Index struct {
	tableDefImpl
	IfNotExists bool
	KeyParts    []*KeyPart
	Name        string
	KeyType     IndexType
	IndexOption *IndexOption
}

func (node *Index) Format(ctx *FmtCtx) {
	ctx.WriteString("index")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	if node.KeyType != INDEX_TYPE_INVALID {
		ctx.WriteString(" using ")
		ctx.WriteString(node.KeyType.ToString())
	}
	if node.KeyParts != nil {
		prefix := " ("
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.IndexOption != nil {
		ctx.WriteByte(' ')
		node.IndexOption.Format(ctx)
	}
}

func NewIndex(k []*KeyPart, n string, t IndexType, io *IndexOption) *Index {
	return &Index{
		KeyParts:    k,
		Name:        n,
		KeyType:     t,
		IndexOption: io,
	}
}

type UniqueIndex struct {
	tableDefImpl
	KeyParts         []*KeyPart
	Name             string
	ConstraintSymbol string
	Empty            bool
	IndexOption      *IndexOption
}

func (node *UniqueIndex) Format(ctx *FmtCtx) {
	if node.ConstraintSymbol != "" {
		ctx.WriteString("constraint " + node.ConstraintSymbol + " ")
	}
	ctx.WriteString("unique key")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	if !node.Empty {
		ctx.WriteString(" using none")
	}
	if node.KeyParts != nil {
		prefix := " ("
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.IndexOption != nil {
		ctx.WriteByte(' ')
		node.IndexOption.Format(ctx)
	}
}

func (node *UniqueIndex) GetIndexName() string {
	if len(node.Name) != 0 {
		return node.Name
	} else {
		return node.ConstraintSymbol
	}
}

func NewUniqueIndex(k []*KeyPart, n string, e bool, io *IndexOption) *UniqueIndex {
	return &UniqueIndex{
		KeyParts:    k,
		Name:        n,
		Empty:       e,
		IndexOption: io,
	}
}

type ForeignKey struct {
	tableDefImpl
	IfNotExists      bool
	KeyParts         []*KeyPart
	Name             string
	ConstraintSymbol string
	Refer            *AttributeReference
	Empty            bool
}

func (node *ForeignKey) Format(ctx *FmtCtx) {
	if node.ConstraintSymbol != "" {
		ctx.WriteString("constraint " + node.ConstraintSymbol + " ")
	}
	ctx.WriteString("foreign key")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	if !node.Empty {
		ctx.WriteString(" using none")
	}
	if node.KeyParts != nil {
		prefix := " ("
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Refer != nil {
		ctx.WriteByte(' ')
		node.Refer.Format(ctx)
	}
}

func NewForeignKey(ine bool, k []*KeyPart, n string, r *AttributeReference, e bool) *ForeignKey {
	return &ForeignKey{
		IfNotExists: ine,
		KeyParts:    k,
		Name:        n,
		Refer:       r,
		Empty:       e,
	}
}

type FullTextIndex struct {
	tableDefImpl
	KeyParts    []*KeyPart
	Name        string
	Empty       bool
	IndexOption *IndexOption
}

func (node *FullTextIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("fulltext")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Name)
	}
	if !node.Empty {
		ctx.WriteString(" using none")
	}
	if node.KeyParts != nil {
		prefix := " ("
		for _, k := range node.KeyParts {
			ctx.WriteString(prefix)
			k.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.IndexOption != nil {
		ctx.WriteByte(' ')
		node.IndexOption.Format(ctx)
	}
}

func NewFullTextIndex(k []*KeyPart, n string, e bool, io *IndexOption) *FullTextIndex {
	return &FullTextIndex{
		KeyParts:    k,
		Name:        n,
		Empty:       e,
		IndexOption: io,
	}
}

type CheckIndex struct {
	tableDefImpl
	Expr     Expr
	Enforced bool
}

func (node *CheckIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("check (")
	node.Expr.Format(ctx)
	ctx.WriteByte(')')
	if node.Enforced {
		ctx.WriteString(" enforced")
	}
}

func NewCheckIndex(e Expr, en bool) *CheckIndex {
	return &CheckIndex{
		Expr:     e,
		Enforced: en,
	}
}

type TableOption interface {
	AlterTableOption
}

type tableOptionImpl struct {
	TableOption
}

type TableOptionProperties struct {
	tableOptionImpl
	Preperties []Property
}

func (node *TableOptionProperties) Format(ctx *FmtCtx) {
	ctx.WriteString("properties")
	if node.Preperties != nil {
		prefix := "("
		for _, p := range node.Preperties {
			ctx.WriteString(prefix)
			p.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

type Property struct {
	Key   string
	Value string
}

func (node *Property) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Key)
	ctx.WriteString(" = ")
	ctx.WriteString(node.Value)
}

type TableOptionEngine struct {
	tableOptionImpl
	Engine string
}

func (node *TableOptionEngine) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = ")
	ctx.WriteString(node.Engine)
}

func NewTableOptionEngine(s string) *TableOptionEngine {
	return &TableOptionEngine{
		Engine: s,
	}
}

type TableOptionSecondaryEngine struct {
	tableOptionImpl
	Engine string
}

func (node *TableOptionSecondaryEngine) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = ")
	ctx.WriteString(node.Engine)
}

func NewTableOptionSecondaryEngine(s string) *TableOptionSecondaryEngine {
	return &TableOptionSecondaryEngine{
		Engine: s,
	}
}

type TableOptionSecondaryEngineNull struct {
	tableOptionImpl
}

func NewTableOptionSecondaryEngineNull() *TableOptionSecondaryEngineNull {
	return &TableOptionSecondaryEngineNull{}
}

type TableOptionCharset struct {
	tableOptionImpl
	Charset string
}

func (node *TableOptionCharset) Format(ctx *FmtCtx) {
	ctx.WriteString("charset = ")
	ctx.WriteString(node.Charset)
}

func NewTableOptionCharset(s string) *TableOptionCharset {
	return &TableOptionCharset{Charset: s}
}

type TableOptionCollate struct {
	tableOptionImpl
	Collate string
}

func (node *TableOptionCollate) Format(ctx *FmtCtx) {
	ctx.WriteString("Collate = ")
	ctx.WriteString(node.Collate)
}

func NewTableOptionCollate(s string) *TableOptionCollate {
	return &TableOptionCollate{
		Collate: s,
	}
}

type TableOptionAutoIncrement struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionAutoIncrement) Format(ctx *FmtCtx) {
	ctx.WriteString("auto_increment = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionAutoIncrement(v uint64) *TableOptionAutoIncrement {
	return &TableOptionAutoIncrement{
		Value: v,
	}
}

type TableOptionComment struct {
	tableOptionImpl
	Comment string
}

func (node *TableOptionComment) Format(ctx *FmtCtx) {
	ctx.WriteString("comment = ")
	ctx.WriteString(node.Comment)
}

func NewTableOptionComment(c string) *TableOptionComment {
	return &TableOptionComment{
		Comment: c,
	}
}

type TableOptionAvgRowLength struct {
	tableOptionImpl
	Length uint64
}

func (node *TableOptionAvgRowLength) Format(ctx *FmtCtx) {
	ctx.WriteString("avg_row_length = ")
	ctx.WriteString(strconv.FormatUint(node.Length, 10))
}

func NewTableOptionAvgRowLength(l uint64) *TableOptionAvgRowLength {
	return &TableOptionAvgRowLength{
		Length: l,
	}
}

type TableOptionChecksum struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionChecksum) Format(ctx *FmtCtx) {
	ctx.WriteString("checksum = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionChecksum(v uint64) *TableOptionChecksum {
	return &TableOptionChecksum{
		Value: v,
	}
}

type TableOptionCompression struct {
	tableOptionImpl
	Compression string
}

func (node *TableOptionCompression) Format(ctx *FmtCtx) {
	ctx.WriteString("compression = ")
	ctx.WriteString(node.Compression)
}

func NewTableOptionCompression(c string) *TableOptionCompression {
	return &TableOptionCompression{
		Compression: c,
	}
}

type TableOptionConnection struct {
	tableOptionImpl
	Connection string
}

func (node *TableOptionConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("connection = ")
	ctx.WriteString(node.Connection)
}

func NewTableOptionConnection(c string) *TableOptionConnection {
	return &TableOptionConnection{
		Connection: c,
	}
}

type TableOptionPassword struct {
	tableOptionImpl
	Password string
}

func (node *TableOptionPassword) Format(ctx *FmtCtx) {
	ctx.WriteString("password = ")
	ctx.WriteString(node.Password)
}

func NewTableOptionPassword(p string) *TableOptionPassword {
	return &TableOptionPassword{
		Password: p,
	}
}

type TableOptionKeyBlockSize struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionKeyBlockSize) Format(ctx *FmtCtx) {
	ctx.WriteString("key_block_size = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionKeyBlockSize(v uint64) *TableOptionKeyBlockSize {
	return &TableOptionKeyBlockSize{
		Value: v,
	}
}

type TableOptionMaxRows struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionMaxRows) Format(ctx *FmtCtx) {
	ctx.WriteString("max_rows = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionMaxRows(v uint64) *TableOptionMaxRows {
	return &TableOptionMaxRows{
		Value: v,
	}
}

type TableOptionMinRows struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionMinRows) Format(ctx *FmtCtx) {
	ctx.WriteString("min_rows = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionMinRows(v uint64) *TableOptionMinRows {
	return &TableOptionMinRows{
		Value: v,
	}
}

type TableOptionDelayKeyWrite struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionDelayKeyWrite) Format(ctx *FmtCtx) {
	ctx.WriteString("key_write = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func NewTableOptionDelayKeyWrite(v uint64) *TableOptionDelayKeyWrite {
	return &TableOptionDelayKeyWrite{
		Value: v,
	}
}

type RowFormatType uint64

const (
	ROW_FORMAT_DEFAULT RowFormatType = iota
	ROW_FORMAT_DYNAMIC
	ROW_FORMAT_FIXED
	ROW_FORMAT_COMPRESSED
	ROW_FORMAT_REDUNDANT
	ROW_FORMAT_COMPACT
)

func (node *RowFormatType) ToString() string {
	switch *node {
	case ROW_FORMAT_DEFAULT:
		return "default"
	case ROW_FORMAT_DYNAMIC:
		return "dynamic"
	case ROW_FORMAT_FIXED:
		return "fixed"
	case ROW_FORMAT_COMPRESSED:
		return "compressed"
	case ROW_FORMAT_REDUNDANT:
		return "redundant"
	case ROW_FORMAT_COMPACT:
		return "compact"
	default:
		return "Unknown RowFormatType"
	}
}

type TableOptionRowFormat struct {
	tableOptionImpl
	Value RowFormatType
}

func (node *TableOptionRowFormat) Format(ctx *FmtCtx) {
	ctx.WriteString("row_format = ")
	ctx.WriteString(node.Value.ToString())
}

func NewTableOptionRowFormat(v RowFormatType) *TableOptionRowFormat {
	return &TableOptionRowFormat{
		Value: v,
	}
}

type TableOptionStatsPersistent struct {
	tableOptionImpl
	Value   uint64
	Default bool
}

func (node *TableOptionStatsPersistent) Format(ctx *FmtCtx) {
	ctx.WriteString("stats_persistent = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatUint(node.Value, 10))
	}
}

func NewTableOptionStatsPersistent() *TableOptionStatsPersistent {
	return &TableOptionStatsPersistent{}
}

type TableOptionStatsAutoRecalc struct {
	tableOptionImpl
	Value   uint64
	Default bool //false -- see Value; true -- Value is useless
}

func (node *TableOptionStatsAutoRecalc) Format(ctx *FmtCtx) {
	ctx.WriteString("stats_auto_recalc = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatUint(node.Value, 10))
	}
}

func NewTableOptionStatsAutoRecalc(v uint64, d bool) *TableOptionStatsAutoRecalc {
	return &TableOptionStatsAutoRecalc{
		Value:   v,
		Default: d,
	}
}

type TableOptionPackKeys struct {
	tableOptionImpl
	Value   int64
	Default bool
}

func (node *TableOptionPackKeys) Format(ctx *FmtCtx) {
	ctx.WriteString("pack_keys = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatInt(node.Value, 10))
	}
}

func NewTableOptionPackKeys(value int64) *TableOptionPackKeys {
	return &TableOptionPackKeys{Value: value}
}

type TableOptionTablespace struct {
	tableOptionImpl
	Name       string
	StorageOpt string
}

func (node *TableOptionTablespace) Format(ctx *FmtCtx) {
	ctx.WriteString("tablespace = ")
	ctx.WriteString(node.Name)
	ctx.WriteString(node.StorageOpt)
}

func NewTableOptionTablespace(n string, s string) *TableOptionTablespace {
	return &TableOptionTablespace{Name: n, StorageOpt: s}
}

type TableOptionDataDirectory struct {
	tableOptionImpl
	Dir string
}

func (node *TableOptionDataDirectory) Format(ctx *FmtCtx) {
	ctx.WriteString("data directory = ")
	ctx.WriteString(node.Dir)
}

func NewTableOptionDataDirectory(d string) *TableOptionDataDirectory {
	return &TableOptionDataDirectory{Dir: d}
}

type TableOptionIndexDirectory struct {
	tableOptionImpl
	Dir string
}

func (node *TableOptionIndexDirectory) Format(ctx *FmtCtx) {
	ctx.WriteString("index directory = ")
	ctx.WriteString(node.Dir)
}

func NewTableOptionIndexDirectory(d string) *TableOptionIndexDirectory {
	return &TableOptionIndexDirectory{
		Dir: d,
	}
}

type TableOptionStorageMedia struct {
	tableOptionImpl
	Media string
}

func (node *TableOptionStorageMedia) Format(ctx *FmtCtx) {
	ctx.WriteString("storage media = ")
	ctx.WriteString(node.Media)
}

func NewTableOptionStorageMedia(m string) *TableOptionStorageMedia {
	return &TableOptionStorageMedia{Media: m}
}

type TableOptionStatsSamplePages struct {
	tableOptionImpl
	Value   uint64
	Default bool //false -- see Value; true -- Value is useless
}

func (node *TableOptionStatsSamplePages) Format(ctx *FmtCtx) {
	ctx.WriteString("stats_sample_pages = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatUint(node.Value, 10))
	}
}

func NewTableOptionStatsSamplePages(v uint64, d bool) *TableOptionStatsSamplePages {
	return &TableOptionStatsSamplePages{
		Value:   v,
		Default: d,
	}
}

type TableOptionUnion struct {
	tableOptionImpl
	Names TableNames
}

func (node *TableOptionUnion) Format(ctx *FmtCtx) {
	ctx.WriteString("union (")
	node.Names.Format(ctx)
	ctx.WriteByte(')')
}

func NewTableOptionUnion(n TableNames) *TableOptionUnion {
	return &TableOptionUnion{Names: n}
}

type TableOptionEncryption struct {
	tableOptionImpl
	Encryption string
}

func (node *TableOptionEncryption) Format(ctx *FmtCtx) {
	ctx.WriteString("encryption = ")
	ctx.WriteString(node.Encryption)
}

func NewTableOptionEncryption(e string) *TableOptionEncryption {
	return &TableOptionEncryption{Encryption: e}
}

type PartitionType interface {
	NodeFormatter
}

type partitionTypeImpl struct {
	PartitionType
}

type HashType struct {
	partitionTypeImpl
	Linear bool
	Expr   Expr
}

func (node *HashType) Format(ctx *FmtCtx) {
	if node.Linear {
		ctx.WriteString("linear ")
	}
	ctx.WriteString("hash")
	if node.Expr != nil {
		ctx.WriteString(" (")
		node.Expr.Format(ctx)
		ctx.WriteByte(')')
	}
}

func NewHashType(l bool, e Expr) *HashType {
	return &HashType{
		Linear: l,
		Expr:   e,
	}
}

type KeyType struct {
	partitionTypeImpl
	Linear     bool
	ColumnList []*UnresolvedName
	Algorithm  int64
}

func (node *KeyType) Format(ctx *FmtCtx) {
	if node.Linear {
		ctx.WriteString("linear ")
	}
	ctx.WriteString("key")
	if node.Algorithm != 0 {
		ctx.WriteString(" algorithm = ")
		ctx.WriteString(strconv.FormatInt(node.Algorithm, 10))
	}
	if node.ColumnList != nil {
		prefix := " ("
		for _, c := range node.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

func NewKeyType(l bool, c []*UnresolvedName) *KeyType {
	return &KeyType{
		Linear:     l,
		ColumnList: c,
	}
}

type RangeType struct {
	partitionTypeImpl
	Expr       Expr
	ColumnList []*UnresolvedName
}

func (node *RangeType) Format(ctx *FmtCtx) {
	ctx.WriteString("range")
	if node.ColumnList != nil {
		prefix := " columns ("
		for _, c := range node.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Expr != nil {
		ctx.WriteString("(")
		node.Expr.Format(ctx)
		ctx.WriteByte(')')
	}
}

func NewRangeType(e Expr, c []*UnresolvedName) *RangeType {
	return &RangeType{
		Expr:       e,
		ColumnList: c,
	}
}

type ListType struct {
	partitionTypeImpl
	Expr       Expr
	ColumnList []*UnresolvedName
}

func (node *ListType) Format(ctx *FmtCtx) {
	ctx.WriteString("list")
	if node.ColumnList != nil {
		prefix := " columns ("
		for _, c := range node.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Expr != nil {
		ctx.WriteString("(")
		node.Expr.Format(ctx)
		ctx.WriteByte(')')
	}
}

func NewListType(e Expr, c []*UnresolvedName) *ListType {
	return &ListType{
		Expr:       e,
		ColumnList: c,
	}
}

type PartitionBy struct {
	IsSubPartition bool // for format
	PType          PartitionType
	Num            uint64
}

func (node *PartitionBy) Format(ctx *FmtCtx) {
	node.PType.Format(ctx)
	if node.Num != 0 {
		if node.IsSubPartition {
			ctx.WriteString(" subpartitions ")
		} else {
			ctx.WriteString(" partitions ")
		}
		ctx.WriteString(strconv.FormatUint(node.Num, 10))
	}
}

func NewPartitionBy(pt PartitionType, n uint64) *PartitionBy {
	return &PartitionBy{
		PType: pt,
		Num:   n,
	}
}

//type SubpartitionBy struct {
//	SubPType PartitionType
//	Num uint64
//}

type Values interface {
	NodeFormatter
}

type valuesImpl struct {
	Values
}

type ValuesLessThan struct {
	valuesImpl
	ValueList Exprs
}

func (node *ValuesLessThan) Format(ctx *FmtCtx) {
	ctx.WriteString("values less than (")
	node.ValueList.Format(ctx)
	ctx.WriteByte(')')
}

func NewValuesLessThan(vl Exprs) *ValuesLessThan {
	return &ValuesLessThan{
		ValueList: vl,
	}
}

type ValuesIn struct {
	valuesImpl
	ValueList Exprs
}

func (node *ValuesIn) Format(ctx *FmtCtx) {
	ctx.WriteString("values in (")
	node.ValueList.Format(ctx)
	ctx.WriteByte(')')
}

func NewValuesIn(vl Exprs) *ValuesIn {
	return &ValuesIn{
		ValueList: vl,
	}
}

type Partition struct {
	Name    Identifier
	Values  Values
	Options []TableOption
	Subs    []*SubPartition
}

func (node *Partition) Format(ctx *FmtCtx) {
	ctx.WriteString("partition ")
	ctx.WriteString(string(node.Name))
	if node.Values != nil {
		ctx.WriteByte(' ')
		node.Values.Format(ctx)
	}
	if node.Options != nil {
		prefix := " "
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
			prefix = " "
		}
	}
	if node.Subs != nil {
		prefix := " ("
		for _, s := range node.Subs {
			ctx.WriteString(prefix)
			s.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

func NewPartition(n Identifier, v Values, o []TableOption, s []*SubPartition) *Partition {
	return &Partition{
		Name:    n,
		Values:  v,
		Options: o,
		Subs:    s,
	}
}

type SubPartition struct {
	Name    Identifier
	Options []TableOption
}

func (node *SubPartition) Format(ctx *FmtCtx) {
	ctx.WriteString("subpartition ")
	ctx.WriteString(string(node.Name))

	if node.Options != nil {
		prefix := " "
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
			prefix = " "
		}
	}
}

func NewSubPartition(n Identifier, o []TableOption) *SubPartition {
	return &SubPartition{
		Name:    n,
		Options: o,
	}
}

type ClusterByOption struct {
	ColumnList []*UnresolvedName
}

type PartitionOption struct {
	PartBy     PartitionBy
	SubPartBy  *PartitionBy
	Partitions []*Partition
}

func (node *PartitionOption) Format(ctx *FmtCtx) {
	ctx.WriteString("partition by ")
	node.PartBy.Format(ctx)
	if node.SubPartBy != nil {
		ctx.WriteString(" subpartition by ")
		node.SubPartBy.Format(ctx)
	}
	if node.Partitions != nil {
		prefix := " ("
		for _, p := range node.Partitions {
			ctx.WriteString(prefix)
			p.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

func NewPartitionOption(pb *PartitionBy, spb *PartitionBy, parts []*Partition) *PartitionOption {
	return &PartitionOption{
		PartBy:     *pb,
		SubPartBy:  spb,
		Partitions: parts,
	}
}

type IndexCategory int

func (ic IndexCategory) ToString() string {
	switch ic {
	case INDEX_CATEGORY_UNIQUE:
		return "unique"
	case INDEX_CATEGORY_FULLTEXT:
		return "fulltext"
	case INDEX_CATEGORY_SPATIAL:
		return "spatial"
	default:
		return "Unknown IndexCategory"
	}
}

const (
	INDEX_CATEGORY_NONE IndexCategory = iota
	INDEX_CATEGORY_UNIQUE
	INDEX_CATEGORY_FULLTEXT
	INDEX_CATEGORY_SPATIAL
)

type CreateIndex struct {
	statementImpl
	Name        Identifier
	Table       TableName
	IndexCat    IndexCategory
	IfNotExists bool
	KeyParts    []*KeyPart
	IndexOption *IndexOption
	MiscOption  []MiscOption
}

func (node *CreateIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("create ")
	if node.IndexCat != INDEX_CATEGORY_NONE {
		ctx.WriteString(node.IndexCat.ToString())
		ctx.WriteByte(' ')
	}

	ctx.WriteString("index ")
	node.Name.Format(ctx)
	ctx.WriteByte(' ')

	if node.IndexOption != nil && node.IndexOption.IType != INDEX_TYPE_INVALID {
		ctx.WriteString("using ")
		ctx.WriteString(node.IndexOption.IType.ToString())
		ctx.WriteByte(' ')
	}

	ctx.WriteString("on ")
	node.Table.Format(ctx)

	ctx.WriteString(" (")
	if node.KeyParts != nil {
		prefix := ""
		for _, kp := range node.KeyParts {
			ctx.WriteString(prefix)
			kp.Format(ctx)
			prefix = ", "
		}
	}
	ctx.WriteString(")")
	if node.IndexOption != nil {
		ctx.WriteByte(' ')
		node.IndexOption.Format(ctx)
	}
}

func (node *CreateIndex) GetStatementType() string { return "Create Index" }
func (node *CreateIndex) GetQueryType() string     { return QueryTypeDDL }

func NewCreateIndex(n Identifier, t TableName, ife bool, it IndexCategory, k []*KeyPart, i *IndexOption, m []MiscOption) *CreateIndex {
	return &CreateIndex{
		Name:        n,
		Table:       t,
		IfNotExists: ife,
		IndexCat:    it,
		KeyParts:    k,
		IndexOption: i,
		MiscOption:  m,
	}
}

type MiscOption interface {
	NodeFormatter
}

type miscOption struct {
	MiscOption
}

type AlgorithmDefault struct {
	miscOption
}

type AlgorithmInplace struct {
	miscOption
}

type AlgorithmCopy struct {
	miscOption
}

type LockDefault struct {
	miscOption
}

type LockNone struct {
	miscOption
}

type LockShared struct {
	miscOption
}

type LockExclusive struct {
	miscOption
}

type CreateRole struct {
	statementImpl
	IfNotExists bool
	Roles       []*Role
}

func (node *CreateRole) Format(ctx *FmtCtx) {
	ctx.WriteString("create role ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	prefix := ""
	for _, r := range node.Roles {
		ctx.WriteString(prefix)
		r.Format(ctx)
		prefix = ", "
	}
}

func (node *CreateRole) GetStatementType() string { return "Create Role" }
func (node *CreateRole) GetQueryType() string     { return QueryTypeDCL }

func NewCreateRole(ife bool, r []*Role) *CreateRole {
	return &CreateRole{
		IfNotExists: ife,
		Roles:       r,
	}
}

type Role struct {
	NodeFormatter
	UserName string
}

func (node *Role) Format(ctx *FmtCtx) {
	ctx.WriteString(node.UserName)
}

func NewRole(u string) *Role {
	return &Role{
		UserName: u,
	}
}

type User struct {
	NodeFormatter
	Username   string
	Hostname   string
	AuthOption *AccountIdentified
}

func (node *User) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Username)
	if node.Hostname != "%" {
		ctx.WriteByte('@')
		ctx.WriteString(node.Hostname)
	}
	if node.AuthOption != nil {
		node.AuthOption.Format(ctx)
	}
}

func NewUser(u, h string) *User {
	return &User{
		Username: u,
		Hostname: h,
	}
}

type UsernameRecord struct {
	Username string
	Hostname string
}

type AuthRecord struct {
	AuthPlugin string
	AuthString string
	HashString string
	ByAuth     bool
}

type TlsOption interface {
	NodeFormatter
}

type tlsOptionImpl struct {
	TlsOption
}

type TlsOptionNone struct {
	tlsOptionImpl
}

func (node *TlsOptionNone) Format(ctx *FmtCtx) {
	ctx.WriteString("none")
}

type TlsOptionSSL struct {
	tlsOptionImpl
}

func (node *TlsOptionSSL) Format(ctx *FmtCtx) {
	ctx.WriteString("ssl")
}

type TlsOptionX509 struct {
	tlsOptionImpl
}

func (node *TlsOptionX509) Format(ctx *FmtCtx) {
	ctx.WriteString("x509")
}

type TlsOptionCipher struct {
	tlsOptionImpl
	Cipher string
}

func (node *TlsOptionCipher) Format(ctx *FmtCtx) {
	ctx.WriteString("cipher ")
	ctx.WriteString(node.Cipher)
}

type TlsOptionIssuer struct {
	tlsOptionImpl
	Issuer string
}

func (node *TlsOptionIssuer) Format(ctx *FmtCtx) {
	ctx.WriteString("issuer ")
	ctx.WriteString(node.Issuer)
}

type TlsOptionSubject struct {
	tlsOptionImpl
	Subject string
}

func (node *TlsOptionSubject) Format(ctx *FmtCtx) {
	ctx.WriteString("subject ")
	ctx.WriteString(node.Subject)
}

type TlsOptionSan struct {
	tlsOptionImpl
	San string
}

func (node *TlsOptionSan) Format(ctx *FmtCtx) {
	ctx.WriteString("san ")
	ctx.WriteString(node.San)
}

type ResourceOption interface {
	NodeFormatter
}

type resourceOptionImpl struct {
	ResourceOption
}

type ResourceOptionMaxQueriesPerHour struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxQueriesPerHour) Format(ctx *FmtCtx) {
	ctx.WriteString("max_queries_per_hour ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

type ResourceOptionMaxUpdatesPerHour struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxUpdatesPerHour) Format(ctx *FmtCtx) {
	ctx.WriteString("max_updates_per_hour ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

type ResourceOptionMaxConnectionPerHour struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxConnectionPerHour) Format(ctx *FmtCtx) {
	ctx.WriteString("max_connections_per_hour ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

type ResourceOptionMaxUserConnections struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxUserConnections) Format(ctx *FmtCtx) {
	ctx.WriteString("max_user_connections ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

type UserMiscOption interface {
	NodeFormatter
}

type userMiscOptionImpl struct {
	UserMiscOption
}

type UserMiscOptionPasswordExpireNone struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordExpireNone) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire")
}

type UserMiscOptionPasswordExpireDefault struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordExpireDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire default")
}

type UserMiscOptionPasswordExpireNever struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordExpireNever) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire never")
}

type UserMiscOptionPasswordExpireInterval struct {
	userMiscOptionImpl
	Value int64
}

func (node *UserMiscOptionPasswordExpireInterval) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire interval ")
	ctx.WriteString(strconv.FormatInt(node.Value, 10))
	ctx.WriteString(" day")
}

type UserMiscOptionPasswordHistoryDefault struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordHistoryDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password history default")
}

type UserMiscOptionPasswordHistoryCount struct {
	userMiscOptionImpl
	Value int64
}

func (node *UserMiscOptionPasswordHistoryCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password history %d", node.Value))
}

type UserMiscOptionPasswordReuseIntervalDefault struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordReuseIntervalDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password reuse interval default")
}

type UserMiscOptionPasswordReuseIntervalCount struct {
	userMiscOptionImpl
	Value int64
}

func (node *UserMiscOptionPasswordReuseIntervalCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password reuse interval %d day", node.Value))
}

type UserMiscOptionPasswordRequireCurrentNone struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordRequireCurrentNone) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current")
}

type UserMiscOptionPasswordRequireCurrentDefault struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordRequireCurrentDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current default")
}

type UserMiscOptionPasswordRequireCurrentOptional struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordRequireCurrentOptional) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current optional")
}

type UserMiscOptionFailedLoginAttempts struct {
	userMiscOptionImpl
	Value int64
}

func (node *UserMiscOptionFailedLoginAttempts) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("failed_login_attempts %d", node.Value))
}

type UserMiscOptionPasswordLockTimeCount struct {
	userMiscOptionImpl
	Value int64
}

func (node *UserMiscOptionPasswordLockTimeCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password_lock_time %d", node.Value))
}

type UserMiscOptionPasswordLockTimeUnbounded struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionPasswordLockTimeUnbounded) Format(ctx *FmtCtx) {
	ctx.WriteString("password_lock_time unbounded")
}

type UserMiscOptionAccountLock struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionAccountLock) Format(ctx *FmtCtx) {
	ctx.WriteString("lock")
}

type UserMiscOptionAccountUnlock struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionAccountUnlock) Format(ctx *FmtCtx) {
	ctx.WriteString("unlock")
}

type CreateUser struct {
	statementImpl
	IfNotExists bool
	Users       []*User
	Role        *Role
	MiscOpt     UserMiscOption
	// comment or attribute
	CommentOrAttribute AccountCommentOrAttribute
}

func (node *CreateUser) Format(ctx *FmtCtx) {
	ctx.WriteString("create user ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	if node.Users != nil {
		prefix := ""
		for _, u := range node.Users {
			ctx.WriteString(prefix)
			u.Format(ctx)
			prefix = ", "
		}
	}

	if node.Role != nil {
		ctx.WriteString(" default role")
		ctx.WriteString(" ")
		node.Role.Format(ctx)
	}

	if node.MiscOpt != nil {
		ctx.WriteString(" ")
		node.MiscOpt.Format(ctx)
	}

	node.CommentOrAttribute.Format(ctx)
}

func (node *CreateUser) GetStatementType() string { return "Create User" }
func (node *CreateUser) GetQueryType() string     { return QueryTypeDCL }

func NewCreateUser(ife bool, u []*User, r *Role, misc UserMiscOption) *CreateUser {
	return &CreateUser{
		IfNotExists: ife,
		Users:       u,
		Role:        r,
		MiscOpt:     misc,
	}
}

type CreateAccount struct {
	statementImpl
	IfNotExists bool
	Name        string
	AuthOption  AccountAuthOption
	//status_option or not
	StatusOption AccountStatus
	//comment or not
	Comment AccountComment
}

func (ca *CreateAccount) Format(ctx *FmtCtx) {
	ctx.WriteString("create account ")
	if ca.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	ctx.WriteString(ca.Name)
	ca.AuthOption.Format(ctx)
	ca.StatusOption.Format(ctx)
	ca.Comment.Format(ctx)
}

func (ca *CreateAccount) GetStatementType() string { return "Create Account" }
func (ca *CreateAccount) GetQueryType() string     { return QueryTypeDCL }

type AccountAuthOption struct {
	Equal          string
	AdminName      string
	IdentifiedType AccountIdentified
}

func (node *AccountAuthOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" admin_name")
	if len(node.Equal) != 0 {
		ctx.WriteString(" ")
		ctx.WriteString(node.Equal)
	}

	ctx.WriteString(fmt.Sprintf(" '%s'", node.AdminName))
	node.IdentifiedType.Format(ctx)

}

type AccountIdentifiedOption int

const (
	AccountIdentifiedByPassword AccountIdentifiedOption = iota
	AccountIdentifiedByRandomPassword
	AccountIdentifiedWithSSL
)

type AccountIdentified struct {
	Typ AccountIdentifiedOption
	Str string
}

func (node *AccountIdentified) Format(ctx *FmtCtx) {
	switch node.Typ {
	case AccountIdentifiedByPassword:
		ctx.WriteString(" identified by '******'")
	case AccountIdentifiedByRandomPassword:
		ctx.WriteString(" identified by random password")
	case AccountIdentifiedWithSSL:
		ctx.WriteString(" identified with '******'")
	}
}

type AccountStatusOption int

const (
	AccountStatusOpen AccountStatusOption = iota
	AccountStatusSuspend
)

func (aso AccountStatusOption) String() string {
	switch aso {
	case AccountStatusOpen:
		return "open"
	case AccountStatusSuspend:
		return "suspend"
	default:
		return "open"
	}
}

type AccountStatus struct {
	Exist  bool
	Option AccountStatusOption
}

func (node *AccountStatus) Format(ctx *FmtCtx) {
	if node.Exist {
		switch node.Option {
		case AccountStatusOpen:
			ctx.WriteString(" open")
		case AccountStatusSuspend:
			ctx.WriteString(" suspend")
		}
	}
}

type AccountComment struct {
	Exist   bool
	Comment string
}

func (node *AccountComment) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(" comment ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Comment))
	}
}

type AccountCommentOrAttribute struct {
	Exist     bool
	IsComment bool
	Str       string
}

func (node *AccountCommentOrAttribute) Format(ctx *FmtCtx) {
	if node.Exist {
		if node.IsComment {
			ctx.WriteString(" comment ")
		} else {
			ctx.WriteString(" attribute ")
		}
		ctx.WriteString(fmt.Sprintf("'%s'", node.Str))
	}
}

type CreatePublication struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	Database    Identifier
	AccountsSet *AccountsSetOption
	Comment     string
}

func (node *CreatePublication) Format(ctx *FmtCtx) {
	ctx.WriteString("create publication ")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" database ")
	node.Database.Format(ctx)
	if node.AccountsSet != nil && len(node.AccountsSet.SetAccounts) > 0 {
		ctx.WriteString(" account ")
		prefix := ""
		for _, a := range node.AccountsSet.SetAccounts {
			ctx.WriteString(prefix)
			a.Format(ctx)
			prefix = ", "
		}
	}
	if len(node.Comment) > 0 {
		ctx.WriteString(" comment ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Comment))
	}
}

func (node *CreatePublication) GetStatementType() string { return "Create Publication" }
func (node *CreatePublication) GetQueryType() string     { return QueryTypeDCL }
