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

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[CreateOptionCharset](
		func() *CreateOptionCharset { return &CreateOptionCharset{} },
		func(c *CreateOptionCharset) { c.reset() },
		reuse.DefaultOptions[CreateOptionCharset](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ClusterByOption](
		func() *ClusterByOption { return &ClusterByOption{} },
		func(c *ClusterByOption) { c.reset() },
		reuse.DefaultOptions[ClusterByOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateOptionCollate](
		func() *CreateOptionCollate { return &CreateOptionCollate{} },
		func(c *CreateOptionCollate) { c.reset() },
		reuse.DefaultOptions[CreateOptionCollate](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateOptionEncryption](
		func() *CreateOptionEncryption { return &CreateOptionEncryption{} },
		func(c *CreateOptionEncryption) { c.reset() },
		reuse.DefaultOptions[CreateOptionEncryption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[SubscriptionOption](
		func() *SubscriptionOption { return &SubscriptionOption{} },
		func(s *SubscriptionOption) { s.reset() },
		reuse.DefaultOptions[SubscriptionOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateDatabase](
		func() *CreateDatabase { return &CreateDatabase{} },
		func(c *CreateDatabase) { c.reset() },
		reuse.DefaultOptions[CreateDatabase](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateTable](
		func() *CreateTable { return &CreateTable{} },
		func(c *CreateTable) { c.reset() },
		reuse.DefaultOptions[CreateTable](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ColumnTableDef](
		func() *ColumnTableDef { return &ColumnTableDef{} },
		func(c *ColumnTableDef) { c.reset() },
		reuse.DefaultOptions[ColumnTableDef](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeNull](
		func() *AttributeNull { return &AttributeNull{} },
		func(a *AttributeNull) { a.reset() },
		reuse.DefaultOptions[AttributeNull](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeDefault](
		func() *AttributeDefault { return &AttributeDefault{} },
		func(a *AttributeDefault) { a.reset() },
		reuse.DefaultOptions[AttributeDefault](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeAutoIncrement](
		func() *AttributeAutoIncrement { return &AttributeAutoIncrement{} },
		func(a *AttributeAutoIncrement) { a.reset() },
		reuse.DefaultOptions[AttributeAutoIncrement](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeUniqueKey](
		func() *AttributeUniqueKey { return &AttributeUniqueKey{} },
		func(a *AttributeUniqueKey) { a.reset() },
		reuse.DefaultOptions[AttributeUniqueKey](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeUnique](
		func() *AttributeUnique { return &AttributeUnique{} },
		func(a *AttributeUnique) { a.reset() },
		reuse.DefaultOptions[AttributeUnique](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeKey](
		func() *AttributeKey { return &AttributeKey{} },
		func(a *AttributeKey) { a.reset() },
		reuse.DefaultOptions[AttributeKey](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributePrimaryKey](
		func() *AttributePrimaryKey { return &AttributePrimaryKey{} },
		func(a *AttributePrimaryKey) { a.reset() },
		reuse.DefaultOptions[AttributePrimaryKey](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeComment](
		func() *AttributeComment { return &AttributeComment{} },
		func(a *AttributeComment) { a.reset() },
		reuse.DefaultOptions[AttributeComment](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeCollate](
		func() *AttributeCollate { return &AttributeCollate{} },
		func(a *AttributeCollate) { a.reset() },
		reuse.DefaultOptions[AttributeCollate](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeColumnFormat](
		func() *AttributeColumnFormat { return &AttributeColumnFormat{} },
		func(a *AttributeColumnFormat) { a.reset() },
		reuse.DefaultOptions[AttributeColumnFormat](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeStorage](
		func() *AttributeStorage { return &AttributeStorage{} },
		func(a *AttributeStorage) { a.reset() },
		reuse.DefaultOptions[AttributeStorage](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeCheckConstraint](
		func() *AttributeCheckConstraint { return &AttributeCheckConstraint{} },
		func(a *AttributeCheckConstraint) { a.reset() },
		reuse.DefaultOptions[AttributeCheckConstraint](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeGeneratedAlways](
		func() *AttributeGeneratedAlways { return &AttributeGeneratedAlways{} },
		func(a *AttributeGeneratedAlways) { a.reset() },
		reuse.DefaultOptions[AttributeGeneratedAlways](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeLowCardinality](
		func() *AttributeLowCardinality { return &AttributeLowCardinality{} },
		func(a *AttributeLowCardinality) { a.reset() },
		reuse.DefaultOptions[AttributeLowCardinality](), //.
	) //WithEnableChecker()

	reuse.CreatePool[KeyPart](
		func() *KeyPart { return &KeyPart{} },
		func(k *KeyPart) { k.reset() },
		reuse.DefaultOptions[KeyPart](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeReference](
		func() *AttributeReference { return &AttributeReference{} },
		func(a *AttributeReference) { a.reset() },
		reuse.DefaultOptions[AttributeReference](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeOnUpdate](
		func() *AttributeOnUpdate { return &AttributeOnUpdate{} },
		func(a *AttributeOnUpdate) { a.reset() },
		reuse.DefaultOptions[AttributeOnUpdate](), //.
	) //WithEnableChecker()

	reuse.CreatePool[IndexOption](
		func() *IndexOption { return &IndexOption{} },
		func(i *IndexOption) { i.reset() },
		reuse.DefaultOptions[IndexOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[PrimaryKeyIndex](
		func() *PrimaryKeyIndex { return &PrimaryKeyIndex{} },
		func(p *PrimaryKeyIndex) { p.reset() },
		reuse.DefaultOptions[PrimaryKeyIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[Index](
		func() *Index { return &Index{} },
		func(i *Index) { i.reset() },
		reuse.DefaultOptions[Index](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UniqueIndex](
		func() *UniqueIndex { return &UniqueIndex{} },
		func(u *UniqueIndex) { u.reset() },
		reuse.DefaultOptions[UniqueIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ForeignKey](
		func() *ForeignKey { return &ForeignKey{} },
		func(f *ForeignKey) { f.reset() },
		reuse.DefaultOptions[ForeignKey](), //.
	) //WithEnableChecker()

	reuse.CreatePool[FullTextIndex](
		func() *FullTextIndex { return &FullTextIndex{} },
		func(f *FullTextIndex) { f.reset() },
		reuse.DefaultOptions[FullTextIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CheckIndex](
		func() *CheckIndex { return &CheckIndex{} },
		func(c *CheckIndex) { c.reset() },
		reuse.DefaultOptions[CheckIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionProperties](
		func() *TableOptionProperties { return &TableOptionProperties{} },
		func(t *TableOptionProperties) { t.reset() },
		reuse.DefaultOptions[TableOptionProperties](), //.
	) //WithEnableChecker()

	reuse.CreatePool[Property](
		func() *Property { return &Property{} },
		func(p *Property) { p.reset() },
		reuse.DefaultOptions[Property](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionEngine](
		func() *TableOptionEngine { return &TableOptionEngine{} },
		func(t *TableOptionEngine) { t.reset() },
		reuse.DefaultOptions[TableOptionEngine](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionEngineAttr](
		func() *TableOptionEngineAttr { return &TableOptionEngineAttr{} },
		func(t *TableOptionEngineAttr) { t.reset() },
		reuse.DefaultOptions[TableOptionEngineAttr](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionInsertMethod](
		func() *TableOptionInsertMethod { return &TableOptionInsertMethod{} },
		func(t *TableOptionInsertMethod) { t.reset() },
		reuse.DefaultOptions[TableOptionInsertMethod](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionSecondaryEngine](
		func() *TableOptionSecondaryEngine { return &TableOptionSecondaryEngine{} },
		func(t *TableOptionSecondaryEngine) { t.reset() },
		reuse.DefaultOptions[TableOptionSecondaryEngine](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionCharset](
		func() *TableOptionCharset { return &TableOptionCharset{} },
		func(t *TableOptionCharset) { t.reset() },
		reuse.DefaultOptions[TableOptionCharset](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionCollate](
		func() *TableOptionCollate { return &TableOptionCollate{} },
		func(t *TableOptionCollate) { t.reset() },
		reuse.DefaultOptions[TableOptionCollate](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionAUTOEXTEND_SIZE](
		func() *TableOptionAUTOEXTEND_SIZE { return &TableOptionAUTOEXTEND_SIZE{} },
		func(t *TableOptionAUTOEXTEND_SIZE) { t.reset() },
		reuse.DefaultOptions[TableOptionAUTOEXTEND_SIZE](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionAutoIncrement](
		func() *TableOptionAutoIncrement { return &TableOptionAutoIncrement{} },
		func(t *TableOptionAutoIncrement) { t.reset() },
		reuse.DefaultOptions[TableOptionAutoIncrement](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionComment](
		func() *TableOptionComment { return &TableOptionComment{} },
		func(t *TableOptionComment) { t.reset() },
		reuse.DefaultOptions[TableOptionComment](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionAvgRowLength](
		func() *TableOptionAvgRowLength { return &TableOptionAvgRowLength{} },
		func(t *TableOptionAvgRowLength) { t.reset() },
		reuse.DefaultOptions[TableOptionAvgRowLength](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionChecksum](
		func() *TableOptionChecksum { return &TableOptionChecksum{} },
		func(t *TableOptionChecksum) { t.reset() },
		reuse.DefaultOptions[TableOptionChecksum](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionCompression](
		func() *TableOptionCompression { return &TableOptionCompression{} },
		func(t *TableOptionCompression) { t.reset() },
		reuse.DefaultOptions[TableOptionCompression](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionConnection](
		func() *TableOptionConnection { return &TableOptionConnection{} },
		func(t *TableOptionConnection) { t.reset() },
		reuse.DefaultOptions[TableOptionConnection](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionPassword](
		func() *TableOptionPassword { return &TableOptionPassword{} },
		func(t *TableOptionPassword) { t.reset() },
		reuse.DefaultOptions[TableOptionPassword](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionKeyBlockSize](
		func() *TableOptionKeyBlockSize { return &TableOptionKeyBlockSize{} },
		func(t *TableOptionKeyBlockSize) { t.reset() },
		reuse.DefaultOptions[TableOptionKeyBlockSize](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionMaxRows](
		func() *TableOptionMaxRows { return &TableOptionMaxRows{} },
		func(t *TableOptionMaxRows) { t.reset() },
		reuse.DefaultOptions[TableOptionMaxRows](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionMinRows](
		func() *TableOptionMinRows { return &TableOptionMinRows{} },
		func(t *TableOptionMinRows) { t.reset() },
		reuse.DefaultOptions[TableOptionMinRows](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionDelayKeyWrite](
		func() *TableOptionDelayKeyWrite { return &TableOptionDelayKeyWrite{} },
		func(t *TableOptionDelayKeyWrite) { t.reset() },
		reuse.DefaultOptions[TableOptionDelayKeyWrite](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionRowFormat](
		func() *TableOptionRowFormat { return &TableOptionRowFormat{} },
		func(t *TableOptionRowFormat) { t.reset() },
		reuse.DefaultOptions[TableOptionRowFormat](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionStartTrans](
		func() *TableOptionStartTrans { return &TableOptionStartTrans{} },
		func(t *TableOptionStartTrans) { t.reset() },
		reuse.DefaultOptions[TableOptionStartTrans](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionSecondaryEngineAttr](
		func() *TableOptionSecondaryEngineAttr { return &TableOptionSecondaryEngineAttr{} },
		func(t *TableOptionSecondaryEngineAttr) { t.reset() },
		reuse.DefaultOptions[TableOptionSecondaryEngineAttr](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionStatsPersistent](
		func() *TableOptionStatsPersistent { return &TableOptionStatsPersistent{} },
		func(t *TableOptionStatsPersistent) { t.reset() },
		reuse.DefaultOptions[TableOptionStatsPersistent](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionStatsAutoRecalc](
		func() *TableOptionStatsAutoRecalc { return &TableOptionStatsAutoRecalc{} },
		func(t *TableOptionStatsAutoRecalc) { t.reset() },
		reuse.DefaultOptions[TableOptionStatsAutoRecalc](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionPackKeys](
		func() *TableOptionPackKeys { return &TableOptionPackKeys{} },
		func(t *TableOptionPackKeys) { t.reset() },
		reuse.DefaultOptions[TableOptionPackKeys](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionTablespace](
		func() *TableOptionTablespace { return &TableOptionTablespace{} },
		func(t *TableOptionTablespace) { t.reset() },
		reuse.DefaultOptions[TableOptionTablespace](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionDataDirectory](
		func() *TableOptionDataDirectory { return &TableOptionDataDirectory{} },
		func(t *TableOptionDataDirectory) { t.reset() },
		reuse.DefaultOptions[TableOptionDataDirectory](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionIndexDirectory](
		func() *TableOptionIndexDirectory { return &TableOptionIndexDirectory{} },
		func(t *TableOptionIndexDirectory) { t.reset() },
		reuse.DefaultOptions[TableOptionIndexDirectory](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionStorageMedia](
		func() *TableOptionStorageMedia { return &TableOptionStorageMedia{} },
		func(t *TableOptionStorageMedia) { t.reset() },
		reuse.DefaultOptions[TableOptionStorageMedia](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionStatsSamplePages](
		func() *TableOptionStatsSamplePages { return &TableOptionStatsSamplePages{} },
		func(t *TableOptionStatsSamplePages) { t.reset() },
		reuse.DefaultOptions[TableOptionStatsSamplePages](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionUnion](
		func() *TableOptionUnion { return &TableOptionUnion{} },
		func(t *TableOptionUnion) { t.reset() },
		reuse.DefaultOptions[TableOptionUnion](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TableOptionEncryption](
		func() *TableOptionEncryption { return &TableOptionEncryption{} },
		func(t *TableOptionEncryption) { t.reset() },
		reuse.DefaultOptions[TableOptionEncryption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[HashType](
		func() *HashType { return &HashType{} },
		func(h *HashType) { h.reset() },
		reuse.DefaultOptions[HashType](), //.
	) //WithEnableChecker()

	reuse.CreatePool[KeyType](
		func() *KeyType { return &KeyType{} },
		func(k *KeyType) { k.reset() },
		reuse.DefaultOptions[KeyType](), //.
	) //WithEnableChecker()

	reuse.CreatePool[RangeType](
		func() *RangeType { return &RangeType{} },
		func(r *RangeType) { r.reset() },
		reuse.DefaultOptions[RangeType](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ListType](
		func() *ListType { return &ListType{} },
		func(l *ListType) { l.reset() },
		reuse.DefaultOptions[ListType](), //.
	) //WithEnableChecker()

	reuse.CreatePool[PartitionBy](
		func() *PartitionBy { return &PartitionBy{} },
		func(p *PartitionBy) { p.reset() },
		reuse.DefaultOptions[PartitionBy](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ValuesLessThan](
		func() *ValuesLessThan { return &ValuesLessThan{} },
		func(v *ValuesLessThan) { v.reset() },
		reuse.DefaultOptions[ValuesLessThan](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ValuesIn](
		func() *ValuesIn { return &ValuesIn{} },
		func(v *ValuesIn) { v.reset() },
		reuse.DefaultOptions[ValuesIn](), //.
	) //WithEnableChecker()

	reuse.CreatePool[Partition](
		func() *Partition { return &Partition{} },
		func(p *Partition) { p.reset() },
		reuse.DefaultOptions[Partition](), //.
	) //WithEnableChecker()

	reuse.CreatePool[SubPartition](
		func() *SubPartition { return &SubPartition{} },
		func(s *SubPartition) { s.reset() },
		reuse.DefaultOptions[SubPartition](), //.
	) //WithEnableChecker()

	reuse.CreatePool[PartitionOption](
		func() *PartitionOption { return &PartitionOption{} },
		func(p *PartitionOption) { p.reset() },
		reuse.DefaultOptions[PartitionOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateIndex](
		func() *CreateIndex { return &CreateIndex{} },
		func(c *CreateIndex) { c.reset() },
		reuse.DefaultOptions[CreateIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateRole](
		func() *CreateRole { return &CreateRole{} },
		func(c *CreateRole) { c.reset() },
		reuse.DefaultOptions[CreateRole](), //.
	) //WithEnableChecker()

	reuse.CreatePool[Role](
		func() *Role { return &Role{} },
		func(r *Role) { r.reset() },
		reuse.DefaultOptions[Role](), //.
	) //WithEnableChecker()

	reuse.CreatePool[User](
		func() *User { return &User{} },
		func(u *User) { u.reset() },
		reuse.DefaultOptions[User](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionNone](
		func() *TlsOptionNone { return &TlsOptionNone{} },
		func(t *TlsOptionNone) { t.reset() },
		reuse.DefaultOptions[TlsOptionNone](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionSSL](
		func() *TlsOptionSSL { return &TlsOptionSSL{} },
		func(t *TlsOptionSSL) { t.reset() },
		reuse.DefaultOptions[TlsOptionSSL](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionX509](
		func() *TlsOptionX509 { return &TlsOptionX509{} },
		func(t *TlsOptionX509) { t.reset() },
		reuse.DefaultOptions[TlsOptionX509](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionCipher](
		func() *TlsOptionCipher { return &TlsOptionCipher{} },
		func(t *TlsOptionCipher) { t.reset() },
		reuse.DefaultOptions[TlsOptionCipher](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionIssuer](
		func() *TlsOptionIssuer { return &TlsOptionIssuer{} },
		func(t *TlsOptionIssuer) { t.reset() },
		reuse.DefaultOptions[TlsOptionIssuer](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionSubject](
		func() *TlsOptionSubject { return &TlsOptionSubject{} },
		func(t *TlsOptionSubject) { t.reset() },
		reuse.DefaultOptions[TlsOptionSubject](), //.
	) //WithEnableChecker()

	reuse.CreatePool[TlsOptionSan](
		func() *TlsOptionSan { return &TlsOptionSan{} },
		func(t *TlsOptionSan) { t.reset() },
		reuse.DefaultOptions[TlsOptionSan](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ResourceOptionMaxQueriesPerHour](
		func() *ResourceOptionMaxQueriesPerHour { return &ResourceOptionMaxQueriesPerHour{} },
		func(r *ResourceOptionMaxQueriesPerHour) { r.reset() },
		reuse.DefaultOptions[ResourceOptionMaxQueriesPerHour](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ResourceOptionMaxUpdatesPerHour](
		func() *ResourceOptionMaxUpdatesPerHour { return &ResourceOptionMaxUpdatesPerHour{} },
		func(r *ResourceOptionMaxUpdatesPerHour) { r.reset() },
		reuse.DefaultOptions[ResourceOptionMaxUpdatesPerHour](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ResourceOptionMaxConnectionPerHour](
		func() *ResourceOptionMaxConnectionPerHour { return &ResourceOptionMaxConnectionPerHour{} },
		func(r *ResourceOptionMaxConnectionPerHour) { r.reset() },
		reuse.DefaultOptions[ResourceOptionMaxConnectionPerHour](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ResourceOptionMaxUserConnections](
		func() *ResourceOptionMaxUserConnections { return &ResourceOptionMaxUserConnections{} },
		func(r *ResourceOptionMaxUserConnections) { r.reset() },
		reuse.DefaultOptions[ResourceOptionMaxUserConnections](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordExpireNone](
		func() *UserMiscOptionPasswordExpireNone { return &UserMiscOptionPasswordExpireNone{} },
		func(u *UserMiscOptionPasswordExpireNone) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordExpireNone](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordExpireDefault](
		func() *UserMiscOptionPasswordExpireDefault { return &UserMiscOptionPasswordExpireDefault{} },
		func(u *UserMiscOptionPasswordExpireDefault) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordExpireDefault](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordExpireNever](
		func() *UserMiscOptionPasswordExpireNever { return &UserMiscOptionPasswordExpireNever{} },
		func(u *UserMiscOptionPasswordExpireNever) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordExpireNever](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordExpireInterval](
		func() *UserMiscOptionPasswordExpireInterval { return &UserMiscOptionPasswordExpireInterval{} },
		func(u *UserMiscOptionPasswordExpireInterval) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordExpireInterval](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordHistoryDefault](
		func() *UserMiscOptionPasswordHistoryDefault { return &UserMiscOptionPasswordHistoryDefault{} },
		func(u *UserMiscOptionPasswordHistoryDefault) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordHistoryDefault](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordHistoryCount](
		func() *UserMiscOptionPasswordHistoryCount { return &UserMiscOptionPasswordHistoryCount{} },
		func(u *UserMiscOptionPasswordHistoryCount) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordHistoryCount](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordReuseIntervalDefault](
		func() *UserMiscOptionPasswordReuseIntervalDefault {
			return &UserMiscOptionPasswordReuseIntervalDefault{}
		},
		func(u *UserMiscOptionPasswordReuseIntervalDefault) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordReuseIntervalDefault](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordReuseIntervalCount](
		func() *UserMiscOptionPasswordReuseIntervalCount { return &UserMiscOptionPasswordReuseIntervalCount{} },
		func(u *UserMiscOptionPasswordReuseIntervalCount) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordReuseIntervalCount](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordRequireCurrentNone](
		func() *UserMiscOptionPasswordRequireCurrentNone { return &UserMiscOptionPasswordRequireCurrentNone{} },
		func(u *UserMiscOptionPasswordRequireCurrentNone) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordRequireCurrentNone](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordRequireCurrentDefault](
		func() *UserMiscOptionPasswordRequireCurrentDefault {
			return &UserMiscOptionPasswordRequireCurrentDefault{}
		},
		func(u *UserMiscOptionPasswordRequireCurrentDefault) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordRequireCurrentDefault](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordRequireCurrentOptional](
		func() *UserMiscOptionPasswordRequireCurrentOptional {
			return &UserMiscOptionPasswordRequireCurrentOptional{}
		},
		func(u *UserMiscOptionPasswordRequireCurrentOptional) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordRequireCurrentOptional](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionFailedLoginAttempts](
		func() *UserMiscOptionFailedLoginAttempts { return &UserMiscOptionFailedLoginAttempts{} },
		func(u *UserMiscOptionFailedLoginAttempts) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionFailedLoginAttempts](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordLockTimeCount](
		func() *UserMiscOptionPasswordLockTimeCount { return &UserMiscOptionPasswordLockTimeCount{} },
		func(u *UserMiscOptionPasswordLockTimeCount) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordLockTimeCount](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionPasswordLockTimeUnbounded](
		func() *UserMiscOptionPasswordLockTimeUnbounded { return &UserMiscOptionPasswordLockTimeUnbounded{} },
		func(u *UserMiscOptionPasswordLockTimeUnbounded) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionPasswordLockTimeUnbounded](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionAccountLock](
		func() *UserMiscOptionAccountLock { return &UserMiscOptionAccountLock{} },
		func(u *UserMiscOptionAccountLock) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionAccountLock](), //.
	) //WithEnableChecker()

	reuse.CreatePool[UserMiscOptionAccountUnlock](
		func() *UserMiscOptionAccountUnlock { return &UserMiscOptionAccountUnlock{} },
		func(u *UserMiscOptionAccountUnlock) { u.reset() },
		reuse.DefaultOptions[UserMiscOptionAccountUnlock](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateUser](
		func() *CreateUser { return &CreateUser{} },
		func(c *CreateUser) { c.reset() },
		reuse.DefaultOptions[CreateUser](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateAccount](
		func() *CreateAccount { return &CreateAccount{} },
		func(c *CreateAccount) { c.reset() },
		reuse.DefaultOptions[CreateAccount](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AccountAuthOption](
		func() *AccountAuthOption { return &AccountAuthOption{} },
		func(a *AccountAuthOption) { a.reset() },
		reuse.DefaultOptions[AccountAuthOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AccountIdentified](
		func() *AccountIdentified { return &AccountIdentified{} },
		func(a *AccountIdentified) { a.reset() },
		reuse.DefaultOptions[AccountIdentified](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AccountStatus](
		func() *AccountStatus { return &AccountStatus{} },
		func(a *AccountStatus) { a.reset() },
		reuse.DefaultOptions[AccountStatus](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AccountComment](
		func() *AccountComment { return &AccountComment{} },
		func(a *AccountComment) { a.reset() },
		reuse.DefaultOptions[AccountComment](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AccountCommentOrAttribute](
		func() *AccountCommentOrAttribute { return &AccountCommentOrAttribute{} },
		func(a *AccountCommentOrAttribute) { a.reset() },
		reuse.DefaultOptions[AccountCommentOrAttribute](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreatePublication](
		func() *CreatePublication { return &CreatePublication{} },
		func(c *CreatePublication) { c.reset() },
		reuse.DefaultOptions[CreatePublication](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeVisable](
		func() *AttributeVisable { return &AttributeVisable{} },
		func(a *AttributeVisable) { a.reset() },
		reuse.DefaultOptions[AttributeVisable](), //.
	) //WithEnableChecker()
}

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

func (node CreateOptionCharset) TypeName() string { return "tree.CreateOptionCharset" }

func (node *CreateOptionCharset) reset() {
	*node = CreateOptionCharset{}
}

func (node *CreateOptionCharset) Free() {
	reuse.Free[CreateOptionCharset](node, nil)
}

func NewCreateOptionCharset(i bool, c string) *CreateOptionCharset {
	cc := reuse.Alloc[CreateOptionCharset](nil)
	cc.IsDefault = i
	cc.Charset = c
	return cc
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

func (node CreateOptionCollate) TypeName() string { return "tree.CreateOptionCollate" }

func (node *CreateOptionCollate) reset() {
	*node = CreateOptionCollate{}
}

func (node *CreateOptionCollate) Free() {
	reuse.Free[CreateOptionCollate](node, nil)
}

func NewCreateOptionCollate(i bool, c string) *CreateOptionCollate {
	cc := reuse.Alloc[CreateOptionCollate](nil)
	cc.IsDefault = i
	cc.Collate = c
	return cc
}

type CreateOptionEncryption struct {
	createOptionImpl
	Encrypt string
}

func (node *CreateOptionEncryption) Format(ctx *FmtCtx) {
	ctx.WriteString("encryption ")
	ctx.WriteString(node.Encrypt)
}

func (node CreateOptionEncryption) TypeName() string { return "tree.CreateOptionEncryption" }

func (node *CreateOptionEncryption) reset() {
	*node = CreateOptionEncryption{}
}

func (node *CreateOptionEncryption) Free() {
	reuse.Free[CreateOptionEncryption](node, nil)
}

func NewCreateOptionEncryption(e string) *CreateOptionEncryption {
	cr := reuse.Alloc[CreateOptionEncryption](nil)
	cr.Encrypt = e
	return cr
}

type SubscriptionOption struct {
	statementImpl
	From        Identifier
	Publication Identifier
}

func NewSubscriptionOption(from Identifier, publication Identifier) *SubscriptionOption {
	s := reuse.Alloc[SubscriptionOption](nil)
	s.From = from
	s.Publication = publication
	return s
}

func (node *SubscriptionOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" from ")
	node.From.Format(ctx)
	ctx.WriteString(" publication ")
	node.Publication.Format(ctx)
}

func (node SubscriptionOption) TypeName() string { return "tree.SubscriptionOption" }

func (node *SubscriptionOption) reset() {
	*node = SubscriptionOption{}
}

func (node *SubscriptionOption) Free() {
	reuse.Free[SubscriptionOption](node, nil)
}

type CreateDatabase struct {
	statementImpl
	IfNotExists        bool
	Name               Identifier
	CreateOptions      []CreateOption
	SubscriptionOption *SubscriptionOption
	Sql                string
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

func (node CreateDatabase) TypeName() string { return "tree.CreateDatabase" }

func (node *CreateDatabase) reset() {
	if node.CreateOptions != nil {
		for _, item := range node.CreateOptions {
			switch opt := item.(type) {
			case *CreateOptionCharset:
				opt.Free()
			case *CreateOptionCollate:
				opt.Free()
			case *CreateOptionEncryption:
				opt.Free()
			}
		}
	}
	if node.SubscriptionOption != nil {
		node.SubscriptionOption.Free()
	}
	*node = CreateDatabase{}
}

func (node *CreateDatabase) Free() {
	reuse.Free[CreateDatabase](node, nil)
}

func NewCreateDatabase(ine bool, name Identifier, sub *SubscriptionOption, opts []CreateOption) *CreateDatabase {
	cr := reuse.Alloc[CreateDatabase](nil)
	cr.IfNotExists = ine
	cr.Name = name
	cr.SubscriptionOption = sub
	cr.CreateOptions = opts
	return cr
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
	AsSource        *Select
	IsDynamicTable  bool
	DTOptions       []TableOption
	IsAsSelect      bool
}

func NewCreateTable() *CreateTable {
	return reuse.Alloc[CreateTable](nil)
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
	if node.IsDynamicTable {
		ctx.WriteString(" dynamic")
	}

	ctx.WriteString(" table")

	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}

	ctx.WriteByte(' ')
	node.Table.Format(ctx)

	if node.IsDynamicTable {
		ctx.WriteString(" as ")
		node.AsSource.Format(ctx)

		if node.DTOptions != nil {
			prefix := " with ("
			for _, t := range node.DTOptions {
				ctx.WriteString(prefix)
				t.Format(ctx)
				prefix = ", "
			}
			ctx.WriteByte(')')
		}
	} else {

		ctx.WriteString(" (")
		for i, def := range node.Defs {
			if i != 0 {
				ctx.WriteString(",")
				ctx.WriteByte(' ')
			}
			def.Format(ctx)
		}
		ctx.WriteByte(')')
	}

	if node.IsAsSelect {
		ctx.WriteString(" as ")
		node.AsSource.Format(ctx)
	}

	if node.Options != nil && !node.IsDynamicTable {
		prefix := " "
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
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

func (node *CreateTable) GetQueryType() string { return QueryTypeDDL }

func (node CreateTable) TypeName() string { return "tree.CreateTable" }

func (node *CreateTable) reset() {

	if node.Defs != nil {
		for _, def := range node.Defs {
			switch d := def.(type) {
			case *ColumnTableDef:
				d.Free()
			case *PrimaryKeyIndex:
				d.Free()
			case *Index:
				d.Free()
			case *UniqueIndex:
				d.Free()
			case *ForeignKey:
				d.Free()
			case *FullTextIndex:
				d.Free()
			case *CheckIndex:
				d.Free()
			default:
				if d != nil {
					panic(fmt.Sprintf("miss Free for %v", def))
				}
			}
		}
	}

	if node.Options != nil {
		for _, item := range node.Options {
			switch opt := item.(type) {
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}

	if node.PartitionOption != nil {
		node.PartitionOption.Free()
	}

	if node.ClusterByOption != nil {
		node.ClusterByOption.Free()
	}

	// if node.AsSource != nil {
	// 	reuse.Free[Select](node.AsSource, nil)
	// }

	if node.DTOptions != nil {
		for _, item := range node.DTOptions {
			switch opt := item.(type) {
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}

	*node = CreateTable{}
}

func (node *CreateTable) Free() {
	reuse.Free[CreateTable](node, nil)
}

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
			if a != nil {
				a.Format(ctx)
			}
		}
	}
}

func (node ColumnTableDef) TypeName() string { return "tree.ColumnTableDef" }

func (node *ColumnTableDef) reset() {
	// if node.Name != nil {
	// 	reuse.Free[UnresolvedName](node.Name, nil)
	// }
	if node.Attributes != nil {
		for _, item := range node.Attributes {
			switch opt := item.(type) {
			case *AttributeNull:
				opt.Free()
			case *AttributeDefault:
				opt.Free()
			case *AttributeAutoIncrement:
				opt.Free()
			case *AttributeUniqueKey:
				opt.Free()
			case *AttributeUnique:
				opt.Free()
			case *AttributeKey:
				opt.Free()
			case *AttributePrimaryKey:
				opt.Free()
			case *AttributeComment:
				opt.Free()
			case *AttributeCollate:
				opt.Free()
			case *AttributeColumnFormat:
				opt.Free()
			case *AttributeStorage:
				opt.Free()
			case *AttributeCheckConstraint:
				opt.Free()
			case *AttributeGeneratedAlways:
				opt.Free()
			case *AttributeLowCardinality:
				opt.Free()
			case *AttributeReference:
				opt.Free()
			case *AttributeAutoRandom:
				panic("currently not used")
			case *AttributeOnUpdate:
				opt.Free()
			case *AttributeVisable:
				opt.Free()
			case *KeyPart:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}
	*node = ColumnTableDef{}
}

func (node *ColumnTableDef) Free() {
	reuse.Free[ColumnTableDef](node, nil)
}

func NewColumnTableDef(n *UnresolvedName, t ResolvableTypeReference, a []ColumnAttribute) *ColumnTableDef {
	co := reuse.Alloc[ColumnTableDef](nil)
	co.Name = n
	co.Type = t
	co.Attributes = a
	return co
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
	Is bool // true NULL (default); false NOT NULL
}

func (node *AttributeNull) Format(ctx *FmtCtx) {
	if node.Is {
		ctx.WriteString("null")
	} else {
		ctx.WriteString("not null")
	}
}

func (node AttributeNull) TypeName() string { return "tree.AttributeNull" }

func (node *AttributeNull) reset() {
	*node = AttributeNull{}
}

func (node *AttributeNull) Free() {
	reuse.Free[AttributeNull](node, nil)
}

func NewAttributeNull(b bool) *AttributeNull {
	a := reuse.Alloc[AttributeNull](nil)
	a.Is = b
	return a
}

type AttributeDefault struct {
	columnAttributeImpl
	Expr Expr
}

func (node *AttributeDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("default ")
	node.Expr.Format(ctx)
}

func (node AttributeDefault) TypeName() string { return "tree.AttributeDefault" }

func (node *AttributeDefault) reset() {
	*node = AttributeDefault{}
}

func (node *AttributeDefault) Free() {
	reuse.Free[AttributeDefault](node, nil)
}

func NewAttributeDefault(e Expr) *AttributeDefault {
	a := reuse.Alloc[AttributeDefault](nil)
	a.Expr = e
	return a
}

type AttributeAutoIncrement struct {
	columnAttributeImpl
	IsAutoIncrement bool
}

func (node *AttributeAutoIncrement) Format(ctx *FmtCtx) {
	ctx.WriteString("auto_increment")
}

func (node AttributeAutoIncrement) TypeName() string { return "tree.AttributeAutoIncrement" }

func (node *AttributeAutoIncrement) reset() {
	*node = AttributeAutoIncrement{}
}

func (node *AttributeAutoIncrement) Free() {
	reuse.Free[AttributeAutoIncrement](node, nil)
}

func NewAttributeAutoIncrement() *AttributeAutoIncrement {
	return reuse.Alloc[AttributeAutoIncrement](nil)
}

type AttributeUniqueKey struct {
	columnAttributeImpl
}

func (node *AttributeUniqueKey) Format(ctx *FmtCtx) {
	ctx.WriteString("unique key")
}

func (node AttributeUniqueKey) TypeName() string { return "tree.AttributeUniqueKey" }

func (node *AttributeUniqueKey) reset() {
	*node = AttributeUniqueKey{}
}

func (node *AttributeUniqueKey) Free() {
	reuse.Free[AttributeUniqueKey](node, nil)
}

func NewAttributeUniqueKey() *AttributeUniqueKey {
	return reuse.Alloc[AttributeUniqueKey](nil)
}

type AttributeUnique struct {
	columnAttributeImpl
}

func (node *AttributeUnique) Format(ctx *FmtCtx) {
	ctx.WriteString("unique")
}

func (node AttributeUnique) TypeName() string { return "tree.AttributeUnique" }

func (node *AttributeUnique) reset() {
	*node = AttributeUnique{}
}

func (node *AttributeUnique) Free() {
	reuse.Free[AttributeUnique](node, nil)
}

func NewAttributeUnique() *AttributeUnique {
	return reuse.Alloc[AttributeUnique](nil)
}

type AttributeKey struct {
	columnAttributeImpl
}

func (node *AttributeKey) Format(ctx *FmtCtx) {
	ctx.WriteString("key")
}

func (node AttributeKey) TypeName() string { return "tree.AttributeKey" }

func (node *AttributeKey) reset() {
	*node = AttributeKey{}
}

func (node *AttributeKey) Free() {
	reuse.Free[AttributeKey](node, nil)
}

func NewAttributeKey() *AttributeKey {
	return reuse.Alloc[AttributeKey](nil)
}

type AttributePrimaryKey struct {
	columnAttributeImpl
}

func (node *AttributePrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString("primary key")
}

func (node AttributePrimaryKey) TypeName() string { return "tree.AttributePrimaryKey" }

func (node *AttributePrimaryKey) reset() {
	*node = AttributePrimaryKey{}
}

func (node *AttributePrimaryKey) Free() {
	reuse.Free[AttributePrimaryKey](node, nil)
}

func NewAttributePrimaryKey() *AttributePrimaryKey {
	return reuse.Alloc[AttributePrimaryKey](nil)
}

type AttributeComment struct {
	columnAttributeImpl
	CMT Expr
}

func (node *AttributeComment) Format(ctx *FmtCtx) {
	ctx.WriteString("comment ")
	node.CMT.Format(ctx)
}

func (node AttributeComment) TypeName() string { return "tree.AttributeComment" }

func (node *AttributeComment) reset() {
	*node = AttributeComment{}
}

func (node *AttributeComment) Free() {
	reuse.Free[AttributeComment](node, nil)
}

func NewAttributeComment(c Expr) *AttributeComment {
	ac := reuse.Alloc[AttributeComment](nil)
	ac.CMT = c
	return ac
}

type AttributeCollate struct {
	columnAttributeImpl
	Collate string
}

func (node *AttributeCollate) Format(ctx *FmtCtx) {
	ctx.WriteString("collate ")
	ctx.WriteString(node.Collate)
}

func (node AttributeCollate) TypeName() string { return "tree.AttributeCollate" }

func (node *AttributeCollate) reset() {
	*node = AttributeCollate{}
}

func (node *AttributeCollate) Free() {
	reuse.Free[AttributeCollate](node, nil)
}

func NewAttributeCollate(c string) *AttributeCollate {
	ac := reuse.Alloc[AttributeCollate](nil)
	ac.Collate = c
	return ac
}

type AttributeColumnFormat struct {
	columnAttributeImpl
	ColumnFormat string
}

func (node *AttributeColumnFormat) Format(ctx *FmtCtx) {
	ctx.WriteString("format ")
	ctx.WriteString(node.ColumnFormat)
}

func (node AttributeColumnFormat) TypeName() string { return "tree.AttributeColumnFormat" }

func (node *AttributeColumnFormat) reset() {
	*node = AttributeColumnFormat{}
}

func (node *AttributeColumnFormat) Free() {
	reuse.Free[AttributeColumnFormat](node, nil)
}

func NewAttributeColumnFormat(f string) *AttributeColumnFormat {
	ac := reuse.Alloc[AttributeColumnFormat](nil)
	ac.ColumnFormat = f
	return ac
}

type AttributeStorage struct {
	columnAttributeImpl
	Storage string
}

func (node *AttributeStorage) Format(ctx *FmtCtx) {
	ctx.WriteString("storage ")
	ctx.WriteString(node.Storage)
}

func (node AttributeStorage) TypeName() string { return "tree.AttributeStorage" }

func (node *AttributeStorage) reset() {
	*node = AttributeStorage{}
}

func (node *AttributeStorage) Free() {
	reuse.Free[AttributeStorage](node, nil)
}

func NewAttributeStorage(s string) *AttributeStorage {
	as := reuse.Alloc[AttributeStorage](nil)
	as.Storage = s
	return as
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

func (node AttributeCheckConstraint) TypeName() string { return "tree.AttributeCheckConstraint" }

func (node *AttributeCheckConstraint) reset() {
	*node = AttributeCheckConstraint{}
}

func (node *AttributeCheckConstraint) Free() {
	reuse.Free[AttributeCheckConstraint](node, nil)
}

func NewAttributeCheckConstraint(e Expr, f bool, n string) *AttributeCheckConstraint {
	ac := reuse.Alloc[AttributeCheckConstraint](nil)
	ac.Name = n
	ac.Expr = e
	ac.Enforced = f
	return ac
}

type AttributeGeneratedAlways struct {
	columnAttributeImpl
	Expr   Expr
	Stored bool
}

func (node *AttributeGeneratedAlways) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
}

func (node AttributeGeneratedAlways) TypeName() string { return "tree.AttributeGeneratedAlways" }

func (node *AttributeGeneratedAlways) reset() {
	*node = AttributeGeneratedAlways{}
}

func (node *AttributeGeneratedAlways) Free() {
	reuse.Free[AttributeGeneratedAlways](node, nil)
}

func NewAttributeGeneratedAlways(e Expr, s bool) *AttributeGeneratedAlways {
	ag := reuse.Alloc[AttributeGeneratedAlways](nil)
	ag.Expr = e
	ag.Stored = s
	return ag
}

type AttributeLowCardinality struct {
	columnAttributeImpl
}

func (node *AttributeLowCardinality) Format(ctx *FmtCtx) {
	ctx.WriteString("low_cardinality")
}

func (node AttributeLowCardinality) TypeName() string { return "tree.AttributeLowCardinality" }

func (node *AttributeLowCardinality) reset() {
	*node = AttributeLowCardinality{}
}

func (node *AttributeLowCardinality) Free() {
	reuse.Free[AttributeLowCardinality](node, nil)
}

func NewAttributeLowCardinality() *AttributeLowCardinality {
	return reuse.Alloc[AttributeLowCardinality](nil)
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

func (node KeyPart) TypeName() string { return "tree.KeyPart" }

func (node *KeyPart) reset() {
	// if node.ColName != nil {
	// 	reuse.Free[UnresolvedName](node.ColName, nil)
	// }
	*node = KeyPart{}
}

func (node *KeyPart) Free() {
	reuse.Free[KeyPart](node, nil)
}

func NewKeyPart(c *UnresolvedName, l int, d Direction, e Expr) *KeyPart {
	kp := reuse.Alloc[KeyPart](nil)
	kp.ColName = c
	kp.Length = l
	kp.Direction = d
	kp.Expr = e
	return kp
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

func (node AttributeReference) TypeName() string { return "tree.AttributeReference" }

func (node *AttributeReference) reset() {
	// if node.TableName != nil {
	// 	reuse.Free[TableName](node.TableName, nil)
	// }
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	*node = AttributeReference{}
}

func (node *AttributeReference) Free() {
	reuse.Free[AttributeReference](node, nil)
}

func NewAttributeReference(t *TableName, kps []*KeyPart, m MatchType,
	od ReferenceOptionType, ou ReferenceOptionType,
) *AttributeReference {
	ar := reuse.Alloc[AttributeReference](nil)
	ar.TableName = t
	ar.KeyParts = kps
	ar.Match = m
	ar.OnDelete = od
	ar.OnUpdate = ou
	return ar
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
	aa := reuse.Alloc[AttributeAutoRandom](nil)
	aa.BitLength = b
	return aa
}

func (node AttributeAutoRandom) TypeName() string { return "tree.AttributeAutoRandom" }

type AttributeOnUpdate struct {
	columnAttributeImpl
	Expr Expr
}

func (node *AttributeOnUpdate) Format(ctx *FmtCtx) {
	ctx.WriteString("on update ")
	node.Expr.Format(ctx)
}

func (node AttributeOnUpdate) TypeName() string { return "tree.AttributeOnUpdate" }

func (node *AttributeOnUpdate) reset() {
	*node = AttributeOnUpdate{}
}

func (node *AttributeOnUpdate) Free() {
	reuse.Free[AttributeOnUpdate](node, nil)
}

func NewAttributeOnUpdate(e Expr) *AttributeOnUpdate {
	ao := reuse.Alloc[AttributeOnUpdate](nil)
	ao.Expr = e
	return ao
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
	case INDEX_TYPE_IVFFLAT:
		return "ivfflat"
	case INDEX_TYPE_MASTER:
		return "master"
	case INDEX_TYPE_INVALID:
		return ""
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
	INDEX_TYPE_IVFFLAT
	INDEX_TYPE_MASTER
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
	AlgoParamList            int64
	AlgoParamVectorOpType    string
}

// Must follow the following sequence when test
func (node *IndexOption) Format(ctx *FmtCtx) {
	if node.KeyBlockSize != 0 || node.ParserName != "" ||
		node.Comment != "" || node.Visible != VISIBLE_TYPE_INVALID ||
		node.AlgoParamList != 0 || node.AlgoParamVectorOpType != "" {
		ctx.WriteByte(' ')
	}
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
	if node.AlgoParamList != 0 {
		ctx.WriteString("LISTS ")
		ctx.WriteString(strconv.FormatInt(node.AlgoParamList, 10))
		ctx.WriteByte(' ')
	}
	if node.AlgoParamVectorOpType != "" {
		ctx.WriteString("OP_TYPE ")
		ctx.WriteString(node.AlgoParamVectorOpType)
		ctx.WriteByte(' ')
	}
	if node.Visible != VISIBLE_TYPE_INVALID {
		ctx.WriteString(node.Visible.ToString())
	}
}

func (node IndexOption) TypeName() string { return "tree.IndexOption" }

func (node *IndexOption) reset() {
	*node = IndexOption{}
}

func (node *IndexOption) Free() {
	reuse.Free[IndexOption](node, nil)
}

func NewIndexOption() *IndexOption {
	return reuse.Alloc[IndexOption](nil)
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

func (node PrimaryKeyIndex) TypeName() string { return "tree.PrimaryKeyIndex" }

func (node *PrimaryKeyIndex) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	if node.IndexOption != nil {
		reuse.Free[IndexOption](node.IndexOption, nil)
	}
	*node = PrimaryKeyIndex{}
}

func (node *PrimaryKeyIndex) Free() {
	reuse.Free[PrimaryKeyIndex](node, nil)
}

func NewPrimaryKeyIndex(k []*KeyPart, n string, e bool, io *IndexOption) *PrimaryKeyIndex {
	pki := reuse.Alloc[PrimaryKeyIndex](nil)
	pki.KeyParts = k
	pki.Name = n
	pki.Empty = e
	pki.IndexOption = io
	return pki
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

func (node Index) TypeName() string { return "tree.Index" }

func (node *Index) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	if node.IndexOption != nil {
		reuse.Free[IndexOption](node.IndexOption, nil)
	}
	*node = Index{}
}

func (node *Index) Free() {
	reuse.Free[Index](node, nil)
}

func NewIndex(ifn bool, k []*KeyPart, n string, t IndexType, io *IndexOption) *Index {
	i := reuse.Alloc[Index](nil)
	i.IfNotExists = ifn
	i.KeyParts = k
	i.Name = n
	i.KeyType = t
	i.IndexOption = io
	return i
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

func (node UniqueIndex) TypeName() string { return "tree.UniqueIndex" }

func (node *UniqueIndex) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	if node.IndexOption != nil {
		reuse.Free[IndexOption](node.IndexOption, nil)
	}
	*node = UniqueIndex{}
}

func (node *UniqueIndex) Free() {
	reuse.Free[UniqueIndex](node, nil)
}

func NewUniqueIndex(k []*KeyPart, n string, e bool, io *IndexOption) *UniqueIndex {
	ui := reuse.Alloc[UniqueIndex](nil)
	ui.KeyParts = k
	ui.Name = n
	ui.Empty = e
	ui.IndexOption = io
	return ui
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

func (node ForeignKey) TypeName() string { return "tree.ForeignKey" }

func (node *ForeignKey) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	if node.Refer != nil {
		reuse.Free[AttributeReference](node.Refer, nil)
	}
	*node = ForeignKey{}
}

func (node *ForeignKey) Free() {
	reuse.Free[ForeignKey](node, nil)
}

func NewForeignKey(ine bool, k []*KeyPart, n string, r *AttributeReference, e bool) *ForeignKey {
	fk := reuse.Alloc[ForeignKey](nil)
	fk.IfNotExists = ine
	fk.KeyParts = k
	fk.Name = n
	fk.Refer = r
	fk.Empty = e
	return fk
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

func (node FullTextIndex) TypeName() string { return "tree.FullTextIndex" }

func (node *FullTextIndex) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			reuse.Free[KeyPart](item, nil)
		}
	}
	if node.IndexOption != nil {
		reuse.Free[IndexOption](node.IndexOption, nil)
	}
	*node = FullTextIndex{}
}

func (node *FullTextIndex) Free() {
	reuse.Free[FullTextIndex](node, nil)
}

func NewFullTextIndex(k []*KeyPart, n string, e bool, io *IndexOption) *FullTextIndex {
	fi := reuse.Alloc[FullTextIndex](nil)
	fi.KeyParts = k
	fi.Name = n
	fi.Empty = e
	fi.IndexOption = io
	return fi
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

func (node CheckIndex) TypeName() string { return "tree.CheckIndex" }

func (node *CheckIndex) reset() {
	*node = CheckIndex{}
}

func (node *CheckIndex) Free() {
	reuse.Free[CheckIndex](node, nil)
}

func NewCheckIndex(e Expr, en bool) *CheckIndex {
	ci := reuse.Alloc[CheckIndex](nil)
	ci.Expr = e
	ci.Enforced = en
	return ci
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

func NewTableOptionProperties(p []Property) *TableOptionProperties {
	tp := reuse.Alloc[TableOptionProperties](nil)
	tp.Preperties = p
	return tp
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

func (node TableOptionProperties) TypeName() string { return "tree.TableOptionProperties" }

func (node *TableOptionProperties) reset() {
	if node.Preperties != nil {
		for _, item := range node.Preperties {
			reuse.Free[Property](&item, nil)
		}
	}
	*node = TableOptionProperties{}
}

func (node *TableOptionProperties) Free() {
	reuse.Free[TableOptionProperties](node, nil)
}

type Property struct {
	Key   string
	Value string
}

func NewProperty(k, v string) *Property {
	p := reuse.Alloc[Property](nil)
	p.Key = k
	p.Value = v
	return p
}

func (node *Property) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Key)
	ctx.WriteString(" = ")
	ctx.WriteString(node.Value)
}

func (node Property) TypeName() string { return "tree.Property" }

func (node *Property) reset() {
	*node = Property{}
}

func (node *Property) Free() {
	reuse.Free[Property](node, nil)
}

type TableOptionEngine struct {
	tableOptionImpl
	Engine string
}

func (node *TableOptionEngine) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = ")
	ctx.WriteString(node.Engine)
}

func (node TableOptionEngine) TypeName() string { return "tree.TableOptionEngine" }

func (node *TableOptionEngine) reset() {
	*node = TableOptionEngine{}
}

func (node *TableOptionEngine) Free() {
	reuse.Free[TableOptionEngine](node, nil)
}

func NewTableOptionEngine(s string) *TableOptionEngine {
	tb := reuse.Alloc[TableOptionEngine](nil)
	tb.Engine = s
	return tb
}

type TableOptionEngineAttr struct {
	tableOptionImpl
	Engine string
}

func (node *TableOptionEngineAttr) Format(ctx *FmtCtx) {
	ctx.WriteString("ENGINE_ATTRIBUTE = ")
	ctx.WriteString(node.Engine)
}

func (node TableOptionEngineAttr) TypeName() string { return "tree.TableOptionEngineAttr" }

func (node *TableOptionEngineAttr) reset() {
	*node = TableOptionEngineAttr{}
}

func (node *TableOptionEngineAttr) Free() {
	reuse.Free[TableOptionEngineAttr](node, nil)
}

func NewTableOptionEngineAttr(s string) *TableOptionEngineAttr {
	tba := reuse.Alloc[TableOptionEngineAttr](nil)
	tba.Engine = s
	return tba
}

type TableOptionInsertMethod struct {
	tableOptionImpl
	Method string
}

func (node *TableOptionInsertMethod) Format(ctx *FmtCtx) {
	ctx.WriteString("INSERT_METHOD = ")
	ctx.WriteString(node.Method)
}

func (node TableOptionInsertMethod) TypeName() string { return "tree.TableOptionInsertMethod" }

func (node *TableOptionInsertMethod) reset() {
	*node = TableOptionInsertMethod{}
}

func (node *TableOptionInsertMethod) Free() {
	reuse.Free[TableOptionInsertMethod](node, nil)
}

func NewTableOptionInsertMethod(s string) *TableOptionInsertMethod {
	t := reuse.Alloc[TableOptionInsertMethod](nil)
	t.Method = s
	return t
}

type TableOptionSecondaryEngine struct {
	tableOptionImpl
	Engine string
}

func (node *TableOptionSecondaryEngine) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = ")
	ctx.WriteString(node.Engine)
}

func (node TableOptionSecondaryEngine) TypeName() string { return "tree.TableOptionSecondaryEngine" }

func (node *TableOptionSecondaryEngine) reset() {
	*node = TableOptionSecondaryEngine{}
}

func (node *TableOptionSecondaryEngine) Free() {
	reuse.Free[TableOptionSecondaryEngine](node, nil)
}

func NewTableOptionSecondaryEngine(s string) *TableOptionSecondaryEngine {
	t := reuse.Alloc[TableOptionSecondaryEngine](nil)
	t.Engine = s
	return t
}

type TableOptionSecondaryEngineNull struct {
	tableOptionImpl
}

func (node TableOptionSecondaryEngineNull) TypeName() string {
	return "tree.TableOptionSecondaryEngineNull"
}

func NewTableOptionSecondaryEngineNull() *TableOptionSecondaryEngineNull {
	return reuse.Alloc[TableOptionSecondaryEngineNull](nil)
}

type TableOptionCharset struct {
	tableOptionImpl
	Charset string
}

func (node *TableOptionCharset) Format(ctx *FmtCtx) {
	ctx.WriteString("charset = ")
	ctx.WriteString(node.Charset)
}

func (node TableOptionCharset) TypeName() string { return "tree.TableOptionCharset" }

func (node *TableOptionCharset) reset() {
	*node = TableOptionCharset{}
}

func (node *TableOptionCharset) Free() {
	reuse.Free[TableOptionCharset](node, nil)
}

func NewTableOptionCharset(s string) *TableOptionCharset {
	t := reuse.Alloc[TableOptionCharset](nil)
	t.Charset = s
	return t
}

type TableOptionCollate struct {
	tableOptionImpl
	Collate string
}

func (node *TableOptionCollate) Format(ctx *FmtCtx) {
	ctx.WriteString("Collate = ")
	ctx.WriteString(node.Collate)
}

func (node TableOptionCollate) TypeName() string { return "tree.TableOptionCollate" }

func (node *TableOptionCollate) reset() {
	*node = TableOptionCollate{}
}

func (node *TableOptionCollate) Free() {
	reuse.Free[TableOptionCollate](node, nil)
}

func NewTableOptionCollate(s string) *TableOptionCollate {
	t := reuse.Alloc[TableOptionCollate](nil)
	t.Collate = s
	return t
}

type TableOptionAUTOEXTEND_SIZE struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionAUTOEXTEND_SIZE) Format(ctx *FmtCtx) {
	ctx.WriteString("AUTOEXTEND_SIZE = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionAUTOEXTEND_SIZE) TypeName() string { return "tree.TableOptionAUTOEXTEND_SIZE" }

func (node *TableOptionAUTOEXTEND_SIZE) reset() {
	*node = TableOptionAUTOEXTEND_SIZE{}
}

func (node *TableOptionAUTOEXTEND_SIZE) Free() {
	reuse.Free[TableOptionAUTOEXTEND_SIZE](node, nil)
}

func NewTableOptionAUTOEXTEND_SIZE(v uint64) *TableOptionAUTOEXTEND_SIZE {
	t := reuse.Alloc[TableOptionAUTOEXTEND_SIZE](nil)
	t.Value = v
	return t
}

type TableOptionAutoIncrement struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionAutoIncrement) Format(ctx *FmtCtx) {
	ctx.WriteString("auto_increment = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionAutoIncrement) TypeName() string { return "tree.TableOptionAutoIncrement" }

func (node *TableOptionAutoIncrement) reset() {
	*node = TableOptionAutoIncrement{}
}

func (node *TableOptionAutoIncrement) Free() {
	reuse.Free[TableOptionAutoIncrement](node, nil)
}

func NewTableOptionAutoIncrement(v uint64) *TableOptionAutoIncrement {
	t := reuse.Alloc[TableOptionAutoIncrement](nil)
	t.Value = v
	return t
}

type TableOptionComment struct {
	tableOptionImpl
	Comment string
}

func (node *TableOptionComment) Format(ctx *FmtCtx) {
	ctx.WriteString("comment = '" + node.Comment + "'")
}

func (node TableOptionComment) TypeName() string { return "tree.TableOptionComment" }

func (node *TableOptionComment) reset() {
	*node = TableOptionComment{}
}

func (node *TableOptionComment) Free() {
	reuse.Free[TableOptionComment](node, nil)
}

func NewTableOptionComment(c string) *TableOptionComment {
	t := reuse.Alloc[TableOptionComment](nil)
	t.Comment = c
	return t
}

type TableOptionAvgRowLength struct {
	tableOptionImpl
	Length uint64
}

func (node *TableOptionAvgRowLength) Format(ctx *FmtCtx) {
	ctx.WriteString("avg_row_length = ")
	ctx.WriteString(strconv.FormatUint(node.Length, 10))
}

func (node TableOptionAvgRowLength) TypeName() string { return "tree.TableOptionAvgRowLength" }

func (node *TableOptionAvgRowLength) reset() {
	*node = TableOptionAvgRowLength{}
}

func (node *TableOptionAvgRowLength) Free() {
	reuse.Free[TableOptionAvgRowLength](node, nil)
}

func NewTableOptionAvgRowLength(l uint64) *TableOptionAvgRowLength {
	t := reuse.Alloc[TableOptionAvgRowLength](nil)
	t.Length = l
	return t
}

type TableOptionChecksum struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionChecksum) Format(ctx *FmtCtx) {
	ctx.WriteString("checksum = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionChecksum) TypeName() string { return "tree.TableOptionChecksum" }

func (node *TableOptionChecksum) reset() {
	*node = TableOptionChecksum{}
}

func (node *TableOptionChecksum) Free() {
	reuse.Free[TableOptionChecksum](node, nil)
}

func NewTableOptionChecksum(v uint64) *TableOptionChecksum {
	t := reuse.Alloc[TableOptionChecksum](nil)
	t.Value = v
	return t
}

type TableOptionCompression struct {
	tableOptionImpl
	Compression string
}

func (node *TableOptionCompression) Format(ctx *FmtCtx) {
	ctx.WriteString("compression = ")
	ctx.WriteString(node.Compression)
}

func (node TableOptionCompression) TypeName() string { return "tree.TableOptionCompression" }

func (node *TableOptionCompression) reset() {
	*node = TableOptionCompression{}
}

func (node *TableOptionCompression) Free() {
	reuse.Free[TableOptionCompression](node, nil)
}

func NewTableOptionCompression(c string) *TableOptionCompression {
	t := reuse.Alloc[TableOptionCompression](nil)
	t.Compression = c
	return t
}

type TableOptionConnection struct {
	tableOptionImpl
	Connection string
}

func (node *TableOptionConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("connection = ")
	ctx.WriteString(node.Connection)
}

func (node TableOptionConnection) TypeName() string { return "tree.TableOptionConnection" }

func (node *TableOptionConnection) reset() {
	*node = TableOptionConnection{}
}

func (node *TableOptionConnection) Free() {
	reuse.Free[TableOptionConnection](node, nil)
}

func NewTableOptionConnection(c string) *TableOptionConnection {
	t := reuse.Alloc[TableOptionConnection](nil)
	t.Connection = c
	return t
}

type TableOptionPassword struct {
	tableOptionImpl
	Password string
}

func (node *TableOptionPassword) Format(ctx *FmtCtx) {
	ctx.WriteString("password = ")
	ctx.WriteString(node.Password)
}

func (node TableOptionPassword) TypeName() string { return "tree.TableOptionPassword" }

func (node *TableOptionPassword) reset() {
	*node = TableOptionPassword{}
}

func (node *TableOptionPassword) Free() {
	reuse.Free[TableOptionPassword](node, nil)
}

func NewTableOptionPassword(p string) *TableOptionPassword {
	t := reuse.Alloc[TableOptionPassword](nil)
	t.Password = p
	return t
}

type TableOptionKeyBlockSize struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionKeyBlockSize) Format(ctx *FmtCtx) {
	ctx.WriteString("key_block_size = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionKeyBlockSize) TypeName() string { return "tree.TableOptionKeyBlockSize" }

func (node *TableOptionKeyBlockSize) reset() {
	*node = TableOptionKeyBlockSize{}
}

func (node *TableOptionKeyBlockSize) Free() {
	reuse.Free[TableOptionKeyBlockSize](node, nil)
}

func NewTableOptionKeyBlockSize(v uint64) *TableOptionKeyBlockSize {
	t := reuse.Alloc[TableOptionKeyBlockSize](nil)
	t.Value = v
	return t
}

type TableOptionMaxRows struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionMaxRows) Format(ctx *FmtCtx) {
	ctx.WriteString("max_rows = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionMaxRows) TypeName() string { return "tree.TableOptionMaxRows" }

func (node *TableOptionMaxRows) reset() {
	*node = TableOptionMaxRows{}
}

func (node *TableOptionMaxRows) Free() {
	reuse.Free[TableOptionMaxRows](node, nil)
}

func NewTableOptionMaxRows(v uint64) *TableOptionMaxRows {
	t := reuse.Alloc[TableOptionMaxRows](nil)
	t.Value = v
	return t
}

type TableOptionMinRows struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionMinRows) Format(ctx *FmtCtx) {
	ctx.WriteString("min_rows = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionMinRows) TypeName() string { return "tree.TableOptionMinRows" }

func (node *TableOptionMinRows) reset() {
	*node = TableOptionMinRows{}
}

func (node *TableOptionMinRows) Free() {
	reuse.Free[TableOptionMinRows](node, nil)
}

func NewTableOptionMinRows(v uint64) *TableOptionMinRows {
	t := reuse.Alloc[TableOptionMinRows](nil)
	t.Value = v
	return t
}

type TableOptionDelayKeyWrite struct {
	tableOptionImpl
	Value uint64
}

func (node *TableOptionDelayKeyWrite) Format(ctx *FmtCtx) {
	ctx.WriteString("key_write = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionDelayKeyWrite) TypeName() string { return "tree.TableOptionDelayKeyWrite" }

func (node *TableOptionDelayKeyWrite) reset() {
	*node = TableOptionDelayKeyWrite{}
}

func (node *TableOptionDelayKeyWrite) Free() {
	reuse.Free[TableOptionDelayKeyWrite](node, nil)
}

func NewTableOptionDelayKeyWrite(v uint64) *TableOptionDelayKeyWrite {
	t := reuse.Alloc[TableOptionDelayKeyWrite](nil)
	t.Value = v
	return t
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

func (node TableOptionRowFormat) TypeName() string { return "tree.TableOptionRowFormat" }

func (node *TableOptionRowFormat) reset() {
	*node = TableOptionRowFormat{}
}

func (node *TableOptionRowFormat) Free() {
	reuse.Free[TableOptionRowFormat](node, nil)
}

func NewTableOptionRowFormat(v RowFormatType) *TableOptionRowFormat {
	t := reuse.Alloc[TableOptionRowFormat](nil)
	t.Value = v
	return t
}

type TableOptionStartTrans struct {
	tableOptionImpl
	Value bool
}

func (node *TableOptionStartTrans) Format(ctx *FmtCtx) {
	ctx.WriteString("START TRANSACTION")
}

func (node TableOptionStartTrans) TypeName() string { return "tree.TableOptionStartTrans" }

func (node *TableOptionStartTrans) reset() {
	*node = TableOptionStartTrans{}
}

func (node *TableOptionStartTrans) Free() {
	reuse.Free[TableOptionStartTrans](node, nil)
}

func NewTTableOptionStartTrans(v bool) *TableOptionStartTrans {
	t := reuse.Alloc[TableOptionStartTrans](nil)
	t.Value = v
	return t
}

type TableOptionSecondaryEngineAttr struct {
	tableOptionImpl
	Attr string
}

func (node *TableOptionSecondaryEngineAttr) Format(ctx *FmtCtx) {
	ctx.WriteString("SECONDARY_ENGINE_ATTRIBUTE = ")
	ctx.WriteString(node.Attr)
}

func (node TableOptionSecondaryEngineAttr) TypeName() string {
	return "tree.TableOptionSecondaryEngineAttr"
}

func (node *TableOptionSecondaryEngineAttr) reset() {
	*node = TableOptionSecondaryEngineAttr{}
}

func (node *TableOptionSecondaryEngineAttr) Free() {
	reuse.Free[TableOptionSecondaryEngineAttr](node, nil)
}

func NewTTableOptionSecondaryEngineAttr(v string) *TableOptionSecondaryEngineAttr {
	t := reuse.Alloc[TableOptionSecondaryEngineAttr](nil)
	t.Attr = v
	return t
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

func (node TableOptionStatsPersistent) TypeName() string { return "tree.TableOptionStatsPersistent" }

func (node *TableOptionStatsPersistent) reset() {
	*node = TableOptionStatsPersistent{}
}

func (node *TableOptionStatsPersistent) Free() {
	reuse.Free[TableOptionStatsPersistent](node, nil)
}

func NewTableOptionStatsPersistent() *TableOptionStatsPersistent {
	return reuse.Alloc[TableOptionStatsPersistent](nil)
}

type TableOptionStatsAutoRecalc struct {
	tableOptionImpl
	Value   uint64
	Default bool // false -- see Value; true -- Value is useless
}

func (node *TableOptionStatsAutoRecalc) Format(ctx *FmtCtx) {
	ctx.WriteString("stats_auto_recalc = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatUint(node.Value, 10))
	}
}

func (node TableOptionStatsAutoRecalc) TypeName() string { return "tree.TableOptionStatsAutoRecalc" }

func (node *TableOptionStatsAutoRecalc) reset() {
	*node = TableOptionStatsAutoRecalc{}
}

func (node *TableOptionStatsAutoRecalc) Free() {
	reuse.Free[TableOptionStatsAutoRecalc](node, nil)
}

func NewTableOptionStatsAutoRecalc() *TableOptionStatsAutoRecalc {
	return reuse.Alloc[TableOptionStatsAutoRecalc](nil)
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

func (node TableOptionPackKeys) TypeName() string { return "tree.TableOptionPackKeys" }

func (node *TableOptionPackKeys) reset() {
	*node = TableOptionPackKeys{}
}

func (node *TableOptionPackKeys) Free() {
	reuse.Free[TableOptionPackKeys](node, nil)
}

func NewTableOptionPackKeys() *TableOptionPackKeys {
	return reuse.Alloc[TableOptionPackKeys](nil)
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

func (node TableOptionTablespace) TypeName() string { return "tree.TableOptionTablespace" }

func (node *TableOptionTablespace) reset() {
	*node = TableOptionTablespace{}
}

func (node *TableOptionTablespace) Free() {
	reuse.Free[TableOptionTablespace](node, nil)
}

func NewTableOptionTablespace(n string, s string) *TableOptionTablespace {
	t := reuse.Alloc[TableOptionTablespace](nil)
	t.Name = n
	t.StorageOpt = s
	return t
}

type TableOptionDataDirectory struct {
	tableOptionImpl
	Dir string
}

func (node *TableOptionDataDirectory) Format(ctx *FmtCtx) {
	ctx.WriteString("data directory = ")
	ctx.WriteString(node.Dir)
}

func (node TableOptionDataDirectory) TypeName() string { return "tree.TableOptionDataDirectory" }

func (node *TableOptionDataDirectory) reset() {
	*node = TableOptionDataDirectory{}
}

func (node *TableOptionDataDirectory) Free() {
	reuse.Free[TableOptionDataDirectory](node, nil)
}

func NewTableOptionDataDirectory(d string) *TableOptionDataDirectory {
	t := reuse.Alloc[TableOptionDataDirectory](nil)
	t.Dir = d
	return t
}

type TableOptionIndexDirectory struct {
	tableOptionImpl
	Dir string
}

func (node *TableOptionIndexDirectory) Format(ctx *FmtCtx) {
	ctx.WriteString("index directory = ")
	ctx.WriteString(node.Dir)
}

func (node TableOptionIndexDirectory) TypeName() string { return "tree.TableOptionIndexDirectory" }

func (node *TableOptionIndexDirectory) reset() {
	*node = TableOptionIndexDirectory{}
}

func (node *TableOptionIndexDirectory) Free() {
	reuse.Free[TableOptionIndexDirectory](node, nil)
}

func NewTableOptionIndexDirectory(d string) *TableOptionIndexDirectory {
	t := reuse.Alloc[TableOptionIndexDirectory](nil)
	t.Dir = d
	return t
}

type TableOptionStorageMedia struct {
	tableOptionImpl
	Media string
}

func (node *TableOptionStorageMedia) Format(ctx *FmtCtx) {
	ctx.WriteString("storage media = ")
	ctx.WriteString(node.Media)
}

func (node TableOptionStorageMedia) TypeName() string { return "tree.TableOptionStorageMedia" }

func (node *TableOptionStorageMedia) reset() {
	*node = TableOptionStorageMedia{}
}

func (node *TableOptionStorageMedia) Free() {
	reuse.Free[TableOptionStorageMedia](node, nil)
}

func NewTableOptionStorageMedia(m string) *TableOptionStorageMedia {
	t := reuse.Alloc[TableOptionStorageMedia](nil)
	t.Media = m
	return t
}

type TableOptionStatsSamplePages struct {
	tableOptionImpl
	Value   uint64
	Default bool // false -- see Value; true -- Value is useless
}

func (node *TableOptionStatsSamplePages) Format(ctx *FmtCtx) {
	ctx.WriteString("stats_sample_pages = ")
	if node.Default {
		ctx.WriteString("default")
	} else {
		ctx.WriteString(strconv.FormatUint(node.Value, 10))
	}
}

func (node TableOptionStatsSamplePages) TypeName() string { return "tree.TableOptionStatsSamplePages" }

func (node *TableOptionStatsSamplePages) reset() {
	*node = TableOptionStatsSamplePages{}
}

func (node *TableOptionStatsSamplePages) Free() {
	reuse.Free[TableOptionStatsSamplePages](node, nil)
}

func NewTableOptionStatsSamplePages() *TableOptionStatsSamplePages {
	return reuse.Alloc[TableOptionStatsSamplePages](nil)
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

func (node TableOptionUnion) TypeName() string { return "tree.TableOptionUnion" }

func (node *TableOptionUnion) reset() {
	*node = TableOptionUnion{}
}

func (node *TableOptionUnion) Free() {
	reuse.Free[TableOptionUnion](node, nil)
}

func NewTableOptionUnion(n TableNames) *TableOptionUnion {
	t := reuse.Alloc[TableOptionUnion](nil)
	t.Names = n
	return t
}

type TableOptionEncryption struct {
	tableOptionImpl
	Encryption string
}

func (node *TableOptionEncryption) Format(ctx *FmtCtx) {
	ctx.WriteString("encryption = ")
	ctx.WriteString(node.Encryption)
}

func (node TableOptionEncryption) TypeName() string { return "tree.TableOptionEncryption" }

func (node *TableOptionEncryption) reset() {
	*node = TableOptionEncryption{}
}

func (node *TableOptionEncryption) Free() {
	reuse.Free[TableOptionEncryption](node, nil)
}

func NewTableOptionEncryption(e string) *TableOptionEncryption {
	t := reuse.Alloc[TableOptionEncryption](nil)
	t.Encryption = e
	return t
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

func (node HashType) TypeName() string { return "tree.HashType" }

func (node *HashType) reset() {
	*node = HashType{}
}

func (node *HashType) Free() {
	reuse.Free[HashType](node, nil)
}

func NewHashType(l bool, e Expr) *HashType {
	h := reuse.Alloc[HashType](nil)
	h.Linear = l
	h.Expr = e
	return h
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

func (node KeyType) TypeName() string { return "tree.KeyType" }

func (node *KeyType) reset() {
	// if node.ColumnList != nil {
	// 	for _, item := range node.ColumnList {
	// 		reuse.Free[UnresolvedName](item, nil)
	// 	}
	// }
	*node = KeyType{}
}

func (node *KeyType) Free() {
	reuse.Free[KeyType](node, nil)
}

func NewKeyType() *KeyType {
	return reuse.Alloc[KeyType](nil)
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

func (node RangeType) TypeName() string { return "tree.RangeType" }

func (node *RangeType) reset() {
	// if node.ColumnList != nil {
	// 	for _, item := range node.ColumnList {
	// 		reuse.Free[UnresolvedName](item, nil)
	// 	}
	// }
	*node = RangeType{}
}

func (node *RangeType) Free() {
	reuse.Free[RangeType](node, nil)
}

func NewRangeType() *RangeType {
	return reuse.Alloc[RangeType](nil)
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

func (node ListType) TypeName() string { return "tree.ListType" }

func (node *ListType) reset() {
	// if node.ColumnList != nil {
	// 	for _, item := range node.ColumnList {
	// 		reuse.Free[UnresolvedName](item, nil)
	// 	}
	// }
	*node = ListType{}
}

func (node *ListType) Free() {
	reuse.Free[ListType](node, nil)
}

func NewListType() *ListType {
	return reuse.Alloc[ListType](nil)
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

func (node PartitionBy) TypeName() string { return "tree.PartitionBy" }

func (node *PartitionBy) reset() {
	switch t := node.PType.(type) {
	case *HashType:
		t.Free()
	case *KeyType:
		t.Free()
	case *RangeType:
		t.Free()
	case *ListType:
		t.Free()
	default:
		if t != nil {
			panic(fmt.Sprintf("miss Free for %v", node.PType))
		}
	}

	*node = PartitionBy{}
}

func (node *PartitionBy) Free() {
	reuse.Free[PartitionBy](node, nil)
}

func NewPartitionBy(typ PartitionType) *PartitionBy {
	pb := reuse.Alloc[PartitionBy](nil)
	pb.PType = typ
	return pb
}

func NewPartitionBy2(issub bool, pt PartitionType, n uint64) *PartitionBy {
	pb := reuse.Alloc[PartitionBy](nil)
	pb.IsSubPartition = issub
	pb.PType = pt
	pb.Num = n
	return pb
}

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

func (node ValuesLessThan) TypeName() string { return "tree.ValuesLessThan" }

func (node *ValuesLessThan) reset() {
	*node = ValuesLessThan{}
}

func (node *ValuesLessThan) Free() {
	reuse.Free[ValuesLessThan](node, nil)
}

func NewValuesLessThan(vl Exprs) *ValuesLessThan {
	v := reuse.Alloc[ValuesLessThan](nil)
	v.ValueList = vl
	return v
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

func (node ValuesIn) TypeName() string { return "tree.ValuesIn" }

func (node *ValuesIn) reset() {
	*node = ValuesIn{}
}

func (node *ValuesIn) Free() {
	reuse.Free[ValuesIn](node, nil)
}

func NewValuesIn(vl Exprs) *ValuesIn {
	vi := reuse.Alloc[ValuesIn](nil)
	vi.ValueList = vl
	return vi
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

func (node Partition) TypeName() string { return "tree.Partition" }

func (node *Partition) reset() {
	if node.Options != nil {
		for _, item := range node.Options {
			switch opt := item.(type) {
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}

	if node.Values != nil {
		switch v := node.Values.(type) {
		case *ValuesLessThan:
			v.Free()
		case *ValuesIn:
			v.Free()
		default:
			if v != nil {
				panic(fmt.Sprintf("miss Free for %v", node.Values))
			}
		}
	}

	if node.Subs != nil {
		for _, item := range node.Subs {
			item.Free()
		}
	}
	*node = Partition{}
}

func (node *Partition) Free() {
	reuse.Free[Partition](node, nil)
}

func NewPartition(n Identifier, v Values, o []TableOption, s []*SubPartition) *Partition {
	p := reuse.Alloc[Partition](nil)
	p.Name = n
	p.Values = v
	p.Options = o
	p.Subs = s
	return p
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

func (node SubPartition) TypeName() string { return "tree.SubPartition" }

func (node *SubPartition) reset() {
	if node.Options != nil {
		for _, item := range node.Options {
			switch opt := item.(type) {
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}
	*node = SubPartition{}
}

func (node *SubPartition) Free() {
	reuse.Free[SubPartition](node, nil)
}

func NewSubPartition(n Identifier, o []TableOption) *SubPartition {
	s := reuse.Alloc[SubPartition](nil)
	s.Name = n
	s.Options = o
	return s
}

type ClusterByOption struct {
	ColumnList []*UnresolvedName
}

func NewClusterByOption(c []*UnresolvedName) *ClusterByOption {
	cb := reuse.Alloc[ClusterByOption](nil)
	cb.ColumnList = c
	return cb
}

func (node ClusterByOption) TypeName() string { return "tree.ClusterByOption" }

func (node *ClusterByOption) reset() {
	// if node.ColumnList != nil {
	// 	for _, item := range node.ColumnList {
	// 		reuse.Free[UnresolvedName](item, nil)
	// 	}
	// }
}

func (node *ClusterByOption) Free() {
	reuse.Free[ClusterByOption](node, nil)
}

type PartitionOption struct {
	statementImpl
	PartBy     *PartitionBy
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

func (node PartitionOption) TypeName() string { return "tree.PartitionOption" }

func (node *PartitionOption) reset() {
	if node.PartBy != nil {
		node.PartBy.Free()
	}
	if node.SubPartBy != nil {
		node.SubPartBy.Free()
	}
	if node.Partitions != nil {
		for _, item := range node.Partitions {
			item.Free()
		}
	}
	*node = PartitionOption{}
}

func (node *PartitionOption) Free() {
	reuse.Free[PartitionOption](node, nil)
}

func NewPartitionOption(pb *PartitionBy, spb *PartitionBy, parts []*Partition) *PartitionOption {
	p := reuse.Alloc[PartitionOption](nil)
	p.PartBy = pb
	p.SubPartBy = spb
	p.Partitions = parts
	return p
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
	Table       *TableName
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
		node.IndexOption.Format(ctx)
	}
}

func (node *CreateIndex) GetStatementType() string { return "Create Index" }
func (node *CreateIndex) GetQueryType() string     { return QueryTypeDDL }

func (node CreateIndex) TypeName() string { return "tree.CreateIndex" }

func (node *CreateIndex) reset() {
	if node.KeyParts != nil {
		for _, item := range node.KeyParts {
			item.Free()
		}
	}
	if node.IndexOption != nil {
		node.IndexOption.Free()
	}

	*node = CreateIndex{}
}

func (node *CreateIndex) Free() {
	reuse.Free[CreateIndex](node, nil)
}

func NewCreateIndex(n Identifier, t *TableName, ife bool, it IndexCategory, k []*KeyPart, i *IndexOption, m []MiscOption) *CreateIndex {
	c := reuse.Alloc[CreateIndex](nil)
	c.Name = n
	c.Table = t
	c.IfNotExists = ife
	c.IndexCat = it
	c.KeyParts = k
	c.IndexOption = i
	c.MiscOption = m
	return c
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

func (node CreateRole) TypeName() string { return "tree.CreateRole" }

func (node *CreateRole) reset() {
	if node.Roles != nil {
		for _, role := range node.Roles {
			role.Free()
		}
	}
	*node = CreateRole{}
}

func (node *CreateRole) Free() {
	reuse.Free[CreateRole](node, nil)
}

func NewCreateRole(ife bool, r []*Role) *CreateRole {
	cr := reuse.Alloc[CreateRole](nil)
	cr.IfNotExists = ife
	cr.Roles = r
	return cr
}

type Role struct {
	NodeFormatter
	UserName string
}

func (node *Role) Format(ctx *FmtCtx) {
	ctx.WriteString(node.UserName)
}

func (node Role) TypeName() string { return "tree.Role" }

func (node *Role) reset() {
	*node = Role{}
}

func (node *Role) Free() {
	// reuse.Free[Role](node, nil)
}

func NewRole(u string) *Role {
	// r := reuse.Alloc[Role](nil)
	r := new(Role)
	r.UserName = u
	return r
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

func (node User) TypeName() string { return "tree.User" }

func (node *User) reset() {
	if node.AuthOption != nil {
		node.AuthOption.Free()
	}
	*node = User{}
}

func (node *User) Free() {
	// if node.AuthOption != nil {
	// 	node.AuthOption.Free()
	// }
	// reuse.Free[User](node, nil)
}

func NewUser(u, h string, a *AccountIdentified) *User {
	// ur := reuse.Alloc[User](nil)
	ur := new(User)
	ur.Username = u
	ur.Hostname = h
	ur.AuthOption = a
	return ur
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

func (node TlsOptionNone) TypeName() string { return "tree.TlsOptionNone" }

func (node *TlsOptionNone) reset() {
	*node = TlsOptionNone{}
}

func (node *TlsOptionNone) Free() {
	reuse.Free[TlsOptionNone](node, nil)
}

type TlsOptionSSL struct {
	tlsOptionImpl
}

func (node *TlsOptionSSL) Format(ctx *FmtCtx) {
	ctx.WriteString("ssl")
}

func (node TlsOptionSSL) TypeName() string { return "tree.TlsOptionSSL" }

func (node *TlsOptionSSL) reset() {
	*node = TlsOptionSSL{}
}

func (node *TlsOptionSSL) Free() {
	reuse.Free[TlsOptionSSL](node, nil)
}

type TlsOptionX509 struct {
	tlsOptionImpl
}

func (node *TlsOptionX509) Format(ctx *FmtCtx) {
	ctx.WriteString("x509")
}

func (node TlsOptionX509) TypeName() string { return "tree.TlsOptionX509" }

func (node *TlsOptionX509) reset() {
	*node = TlsOptionX509{}
}

func (node *TlsOptionX509) Free() {
	reuse.Free[TlsOptionX509](node, nil)
}

type TlsOptionCipher struct {
	tlsOptionImpl
	Cipher string
}

func (node *TlsOptionCipher) Format(ctx *FmtCtx) {
	ctx.WriteString("cipher ")
	ctx.WriteString(node.Cipher)
}

func (node TlsOptionCipher) TypeName() string { return "tree.TlsOptionCipher" }

func (node *TlsOptionCipher) reset() {
	*node = TlsOptionCipher{}
}

func (node *TlsOptionCipher) Free() {
	reuse.Free[TlsOptionCipher](node, nil)
}

type TlsOptionIssuer struct {
	tlsOptionImpl
	Issuer string
}

func (node *TlsOptionIssuer) Format(ctx *FmtCtx) {
	ctx.WriteString("issuer ")
	ctx.WriteString(node.Issuer)
}

func (node TlsOptionIssuer) TypeName() string { return "tree.TlsOptionIssuer" }

func (node *TlsOptionIssuer) reset() {
	*node = TlsOptionIssuer{}
}

func (node *TlsOptionIssuer) Free() {
	reuse.Free[TlsOptionIssuer](node, nil)
}

type TlsOptionSubject struct {
	tlsOptionImpl
	Subject string
}

func (node *TlsOptionSubject) Format(ctx *FmtCtx) {
	ctx.WriteString("subject ")
	ctx.WriteString(node.Subject)
}

func (node TlsOptionSubject) TypeName() string { return "tree.TlsOptionSubject" }

func (node *TlsOptionSubject) reset() {
	*node = TlsOptionSubject{}
}

func (node *TlsOptionSubject) Free() {
	reuse.Free[TlsOptionSubject](node, nil)
}

type TlsOptionSan struct {
	tlsOptionImpl
	San string
}

func (node *TlsOptionSan) Format(ctx *FmtCtx) {
	ctx.WriteString("san ")
	ctx.WriteString(node.San)
}

func (node TlsOptionSan) TypeName() string { return "tree.TlsOptionSan" }

func (node *TlsOptionSan) reset() {
	*node = TlsOptionSan{}
}

func (node *TlsOptionSan) Free() {
	reuse.Free[TlsOptionSan](node, nil)
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

func (node ResourceOptionMaxQueriesPerHour) TypeName() string {
	return "tree.ResourceOptionMaxQueriesPerHour"
}

func (node *ResourceOptionMaxQueriesPerHour) reset() {
	*node = ResourceOptionMaxQueriesPerHour{}
}

func (node *ResourceOptionMaxQueriesPerHour) Free() {
	reuse.Free[ResourceOptionMaxQueriesPerHour](node, nil)
}

type ResourceOptionMaxUpdatesPerHour struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxUpdatesPerHour) Format(ctx *FmtCtx) {
	ctx.WriteString("max_updates_per_hour ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

func (node ResourceOptionMaxUpdatesPerHour) TypeName() string {
	return "tree.ResourceOptionMaxUpdatesPerHour"
}

func (node *ResourceOptionMaxUpdatesPerHour) reset() {
	*node = ResourceOptionMaxUpdatesPerHour{}
}

func (node *ResourceOptionMaxUpdatesPerHour) Free() {
	reuse.Free[ResourceOptionMaxUpdatesPerHour](node, nil)
}

type ResourceOptionMaxConnectionPerHour struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxConnectionPerHour) Format(ctx *FmtCtx) {
	ctx.WriteString("max_connections_per_hour ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

func (node ResourceOptionMaxConnectionPerHour) TypeName() string {
	return "tree.ResourceOptionMaxConnectionPerHour"
}

func (node *ResourceOptionMaxConnectionPerHour) reset() {
	*node = ResourceOptionMaxConnectionPerHour{}
}

func (node *ResourceOptionMaxConnectionPerHour) Free() {
	reuse.Free[ResourceOptionMaxConnectionPerHour](node, nil)
}

type ResourceOptionMaxUserConnections struct {
	resourceOptionImpl
	Count int64
}

func (node *ResourceOptionMaxUserConnections) Format(ctx *FmtCtx) {
	ctx.WriteString("max_user_connections ")
	ctx.WriteString(strconv.FormatInt(node.Count, 10))
}

func (node ResourceOptionMaxUserConnections) TypeName() string {
	return "tree.ResourceOptionMaxUserConnections"
}

func (node *ResourceOptionMaxUserConnections) reset() {
	*node = ResourceOptionMaxUserConnections{}
}

func (node *ResourceOptionMaxUserConnections) Free() {
	reuse.Free[ResourceOptionMaxUserConnections](node, nil)
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

func NewUserMiscOptionPasswordExpireNone() *UserMiscOptionPasswordExpireNone {
	return reuse.Alloc[UserMiscOptionPasswordExpireNone](nil)
}

func (node *UserMiscOptionPasswordExpireNone) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire")
}

func (node UserMiscOptionPasswordExpireNone) TypeName() string {
	return "tree.UserMiscOptionPasswordExpireNone"
}

func (node *UserMiscOptionPasswordExpireNone) reset() {
	*node = UserMiscOptionPasswordExpireNone{}
}

func (node *UserMiscOptionPasswordExpireNone) Free() {
	reuse.Free[UserMiscOptionPasswordExpireNone](node, nil)
}

type UserMiscOptionPasswordExpireDefault struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordExpireDefault() *UserMiscOptionPasswordExpireDefault {
	return reuse.Alloc[UserMiscOptionPasswordExpireDefault](nil)
}

func (node *UserMiscOptionPasswordExpireDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire default")
}

func (node UserMiscOptionPasswordExpireDefault) TypeName() string {
	return "tree.UserMiscOptionPasswordExpireDefault"
}

func (node *UserMiscOptionPasswordExpireDefault) reset() {
	*node = UserMiscOptionPasswordExpireDefault{}
}

func (node *UserMiscOptionPasswordExpireDefault) Free() {
	reuse.Free[UserMiscOptionPasswordExpireDefault](node, nil)
}

type UserMiscOptionPasswordExpireNever struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordExpireNever() *UserMiscOptionPasswordExpireNever {
	return reuse.Alloc[UserMiscOptionPasswordExpireNever](nil)
}

func (node *UserMiscOptionPasswordExpireNever) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire never")
}

func (node UserMiscOptionPasswordExpireNever) TypeName() string {
	return "tree.UserMiscOptionPasswordExpireNever"
}

func (node *UserMiscOptionPasswordExpireNever) reset() {
	*node = UserMiscOptionPasswordExpireNever{}
}

func (node *UserMiscOptionPasswordExpireNever) Free() {
	reuse.Free[UserMiscOptionPasswordExpireNever](node, nil)
}

type UserMiscOptionPasswordExpireInterval struct {
	userMiscOptionImpl
	Value int64
}

func NewUserMiscOptionPasswordExpireInterval(v int64) *UserMiscOptionPasswordExpireInterval {
	pi := reuse.Alloc[UserMiscOptionPasswordExpireInterval](nil)
	pi.Value = v
	return pi
}

func (node *UserMiscOptionPasswordExpireInterval) Format(ctx *FmtCtx) {
	ctx.WriteString("password expire interval ")
	ctx.WriteString(strconv.FormatInt(node.Value, 10))
	ctx.WriteString(" day")
}

func (node UserMiscOptionPasswordExpireInterval) TypeName() string {
	return "tree.UserMiscOptionPasswordExpireInterval"
}

func (node *UserMiscOptionPasswordExpireInterval) reset() {
	*node = UserMiscOptionPasswordExpireInterval{}
}

func (node *UserMiscOptionPasswordExpireInterval) Free() {
	reuse.Free[UserMiscOptionPasswordExpireInterval](node, nil)
}

type UserMiscOptionPasswordHistoryDefault struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordHistoryDefault() *UserMiscOptionPasswordHistoryDefault {
	return reuse.Alloc[UserMiscOptionPasswordHistoryDefault](nil)
}

func (node *UserMiscOptionPasswordHistoryDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password history default")
}

func (node UserMiscOptionPasswordHistoryDefault) TypeName() string {
	return "tree.UserMiscOptionPasswordHistoryDefault"
}

func (node *UserMiscOptionPasswordHistoryDefault) reset() {
	*node = UserMiscOptionPasswordHistoryDefault{}
}

func (node *UserMiscOptionPasswordHistoryDefault) Free() {
	reuse.Free[UserMiscOptionPasswordHistoryDefault](node, nil)
}

type UserMiscOptionPasswordHistoryCount struct {
	userMiscOptionImpl
	Value int64
}

func NewUserMiscOptionPasswordHistoryCount(v int64) *UserMiscOptionPasswordHistoryCount {
	phc := reuse.Alloc[UserMiscOptionPasswordHistoryCount](nil)
	phc.Value = v
	return phc
}

func (node *UserMiscOptionPasswordHistoryCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password history %d", node.Value))
}

func (node UserMiscOptionPasswordHistoryCount) TypeName() string {
	return "tree.UserMiscOptionPasswordHistoryCount"
}

func (node *UserMiscOptionPasswordHistoryCount) reset() {
	*node = UserMiscOptionPasswordHistoryCount{}
}

func (node *UserMiscOptionPasswordHistoryCount) Free() {
	reuse.Free[UserMiscOptionPasswordHistoryCount](node, nil)
}

type UserMiscOptionPasswordReuseIntervalDefault struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordReuseIntervalDefault() *UserMiscOptionPasswordReuseIntervalDefault {
	return reuse.Alloc[UserMiscOptionPasswordReuseIntervalDefault](nil)
}

func (node *UserMiscOptionPasswordReuseIntervalDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password reuse interval default")
}

func (node UserMiscOptionPasswordReuseIntervalDefault) TypeName() string {
	return "tree.UserMiscOptionPasswordReuseIntervalDefault"
}

func (node *UserMiscOptionPasswordReuseIntervalDefault) reset() {
	*node = UserMiscOptionPasswordReuseIntervalDefault{}
}

func (node *UserMiscOptionPasswordReuseIntervalDefault) Free() {
	reuse.Free[UserMiscOptionPasswordReuseIntervalDefault](node, nil)
}

type UserMiscOptionPasswordReuseIntervalCount struct {
	userMiscOptionImpl
	Value int64
}

func NewUserMiscOptionPasswordReuseIntervalCount(v int64) *UserMiscOptionPasswordReuseIntervalCount {
	pr := reuse.Alloc[UserMiscOptionPasswordReuseIntervalCount](nil)
	pr.Value = v
	return pr
}

func (node *UserMiscOptionPasswordReuseIntervalCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password reuse interval %d day", node.Value))
}

func (node UserMiscOptionPasswordReuseIntervalCount) TypeName() string {
	return "tree.UserMiscOptionPasswordReuseIntervalCount"
}

func (node *UserMiscOptionPasswordReuseIntervalCount) reset() {
	*node = UserMiscOptionPasswordReuseIntervalCount{}
}

func (node *UserMiscOptionPasswordReuseIntervalCount) Free() {
	reuse.Free[UserMiscOptionPasswordReuseIntervalCount](node, nil)
}

type UserMiscOptionPasswordRequireCurrentNone struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordRequireCurrentNone() *UserMiscOptionPasswordRequireCurrentNone {
	return reuse.Alloc[UserMiscOptionPasswordRequireCurrentNone](nil)
}

func (node *UserMiscOptionPasswordRequireCurrentNone) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current")
}

func (node UserMiscOptionPasswordRequireCurrentNone) TypeName() string {
	return "tree.UserMiscOptionPasswordRequireCurrentNone"
}

func (node *UserMiscOptionPasswordRequireCurrentNone) reset() {
	*node = UserMiscOptionPasswordRequireCurrentNone{}
}

func (node *UserMiscOptionPasswordRequireCurrentNone) Free() {
	reuse.Free[UserMiscOptionPasswordRequireCurrentNone](node, nil)
}

type UserMiscOptionPasswordRequireCurrentDefault struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordRequireCurrentDefault() *UserMiscOptionPasswordRequireCurrentDefault {
	return reuse.Alloc[UserMiscOptionPasswordRequireCurrentDefault](nil)
}

func (node *UserMiscOptionPasswordRequireCurrentDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current default")
}

func (node UserMiscOptionPasswordRequireCurrentDefault) TypeName() string {
	return "tree.UserMiscOptionPasswordRequireCurrentDefault"
}

func (node *UserMiscOptionPasswordRequireCurrentDefault) reset() {
	*node = UserMiscOptionPasswordRequireCurrentDefault{}
}

func (node *UserMiscOptionPasswordRequireCurrentDefault) Free() {
	reuse.Free[UserMiscOptionPasswordRequireCurrentDefault](node, nil)
}

type UserMiscOptionPasswordRequireCurrentOptional struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordRequireCurrentOptional() *UserMiscOptionPasswordRequireCurrentOptional {
	return reuse.Alloc[UserMiscOptionPasswordRequireCurrentOptional](nil)
}

func (node *UserMiscOptionPasswordRequireCurrentOptional) Format(ctx *FmtCtx) {
	ctx.WriteString("password require current optional")
}

func (node UserMiscOptionPasswordRequireCurrentOptional) TypeName() string {
	return "tree.UserMiscOptionPasswordRequireCurrentOptional"
}

func (node *UserMiscOptionPasswordRequireCurrentOptional) reset() {
	*node = UserMiscOptionPasswordRequireCurrentOptional{}
}

func (node *UserMiscOptionPasswordRequireCurrentOptional) Free() {
	reuse.Free[UserMiscOptionPasswordRequireCurrentOptional](node, nil)
}

type UserMiscOptionFailedLoginAttempts struct {
	userMiscOptionImpl
	Value int64
}

func NewUserMiscOptionFailedLoginAttempts(v int64) *UserMiscOptionFailedLoginAttempts {
	fla := reuse.Alloc[UserMiscOptionFailedLoginAttempts](nil)
	fla.Value = v
	return fla
}

func (node *UserMiscOptionFailedLoginAttempts) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("failed_login_attempts %d", node.Value))
}

func (node UserMiscOptionFailedLoginAttempts) TypeName() string {
	return "tree.UserMiscOptionFailedLoginAttempts"
}

func (node *UserMiscOptionFailedLoginAttempts) reset() {
	*node = UserMiscOptionFailedLoginAttempts{}
}

func (node *UserMiscOptionFailedLoginAttempts) Free() {
	reuse.Free[UserMiscOptionFailedLoginAttempts](node, nil)
}

type UserMiscOptionPasswordLockTimeCount struct {
	userMiscOptionImpl
	Value int64
}

func NewUserMiscOptionPasswordLockTimeCount(v int64) *UserMiscOptionPasswordLockTimeCount {
	ua := reuse.Alloc[UserMiscOptionPasswordLockTimeCount](nil)
	ua.Value = v
	return ua
}

func (node *UserMiscOptionPasswordLockTimeCount) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("password_lock_time %d", node.Value))
}

func (node UserMiscOptionPasswordLockTimeCount) TypeName() string {
	return "tree.UserMiscOptionPasswordLockTimeCount"
}

func (node *UserMiscOptionPasswordLockTimeCount) reset() {
	*node = UserMiscOptionPasswordLockTimeCount{}
}

func (node *UserMiscOptionPasswordLockTimeCount) Free() {
	reuse.Free[UserMiscOptionPasswordLockTimeCount](node, nil)
}

type UserMiscOptionPasswordLockTimeUnbounded struct {
	userMiscOptionImpl
}

func NewUserMiscOptionPasswordLockTimeUnbounded() *UserMiscOptionPasswordLockTimeUnbounded {
	return reuse.Alloc[UserMiscOptionPasswordLockTimeUnbounded](nil)
}

func (node *UserMiscOptionPasswordLockTimeUnbounded) Format(ctx *FmtCtx) {
	ctx.WriteString("password_lock_time unbounded")
}

func (node UserMiscOptionPasswordLockTimeUnbounded) TypeName() string {
	return "tree.UserMiscOptionPasswordLockTimeUnbounded"
}

func (node *UserMiscOptionPasswordLockTimeUnbounded) reset() {
	*node = UserMiscOptionPasswordLockTimeUnbounded{}
}

func (node *UserMiscOptionPasswordLockTimeUnbounded) Free() {
	reuse.Free[UserMiscOptionPasswordLockTimeUnbounded](node, nil)
}

type UserMiscOptionAccountLock struct {
	userMiscOptionImpl
}

func NewUserMiscOptionAccountLock() *UserMiscOptionAccountLock {
	ua := reuse.Alloc[UserMiscOptionAccountLock](nil)
	return ua
}

func (node *UserMiscOptionAccountLock) Format(ctx *FmtCtx) {
	ctx.WriteString("lock")
}

func (node UserMiscOptionAccountLock) TypeName() string { return "tree.UserMiscOptionAccountLock" }

func (node *UserMiscOptionAccountLock) reset() {
	*node = UserMiscOptionAccountLock{}
}

func (node *UserMiscOptionAccountLock) Free() {
	reuse.Free[UserMiscOptionAccountLock](node, nil)
}

type UserMiscOptionAccountUnlock struct {
	userMiscOptionImpl
}

func (node *UserMiscOptionAccountUnlock) Format(ctx *FmtCtx) {
	ctx.WriteString("unlock")
}

func NewUserMiscOptionAccountUnlock() *UserMiscOptionAccountUnlock {
	ua := reuse.Alloc[UserMiscOptionAccountUnlock](nil)
	return ua
}

func (node UserMiscOptionAccountUnlock) TypeName() string { return "tree.UserMiscOptionAccountUnlock" }

func (node *UserMiscOptionAccountUnlock) reset() {
	*node = UserMiscOptionAccountUnlock{}
}

func (node *UserMiscOptionAccountUnlock) Free() {
	reuse.Free[UserMiscOptionAccountUnlock](node, nil)
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

func (node CreateUser) TypeName() string { return "tree.CreateUser" }

func (node *CreateUser) reset() {
	if node.Users != nil {
		for _, item := range node.Users {
			item.Free()
		}
	}

	if node.Role != nil {
		node.Role.Free()
	}

	if node.MiscOpt != nil {
		switch mt := node.MiscOpt.(type) {
		case *UserMiscOptionPasswordExpireNone:
			mt.Free()
		case *UserMiscOptionPasswordExpireDefault:
			mt.Free()
		case *UserMiscOptionPasswordExpireNever:
			mt.Free()
		case *UserMiscOptionPasswordExpireInterval:
			mt.Free()
		case *UserMiscOptionPasswordHistoryDefault:
			mt.Free()
		case *UserMiscOptionPasswordHistoryCount:
			mt.Free()
		case *UserMiscOptionPasswordReuseIntervalDefault:
			mt.Free()
		case *UserMiscOptionPasswordReuseIntervalCount:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentNone:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentDefault:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentOptional:
			mt.Free()
		case *UserMiscOptionFailedLoginAttempts:
			mt.Free()
		case *UserMiscOptionPasswordLockTimeCount:
			mt.Free()
		case *UserMiscOptionPasswordLockTimeUnbounded:
			mt.Free()
		case *UserMiscOptionAccountLock:
			mt.Free()
		case *UserMiscOptionAccountUnlock:
			mt.Free()
		default:
			if mt != nil {
				panic(fmt.Sprintf("miss Free for %v", node.MiscOpt))
			}
		}
	}

	node.CommentOrAttribute.Free()
	*node = CreateUser{}
}

func (node *CreateUser) Free() {
	reuse.Free[CreateUser](node, nil)
}

func NewCreateUser(ife bool, u []*User, r *Role, misc UserMiscOption, c AccountCommentOrAttribute) *CreateUser {
	cu := reuse.Alloc[CreateUser](nil)
	cu.IfNotExists = ife
	cu.Users = u
	cu.Role = r
	cu.MiscOpt = misc
	cu.CommentOrAttribute = c
	return cu
}

type CreateAccount struct {
	statementImpl
	IfNotExists bool
	Name        string
	AuthOption  AccountAuthOption
	// status_option or not
	StatusOption AccountStatus
	// comment or not
	Comment AccountComment
}

func NewCreateAccount(ife bool, n string, ao AccountAuthOption, so AccountStatus, c AccountComment) *CreateAccount {
	ca := reuse.Alloc[CreateAccount](nil)
	ca.IfNotExists = ife
	ca.Name = n
	ca.AuthOption = ao
	ca.StatusOption = so
	ca.Comment = c
	return ca
}

func (node *CreateAccount) Format(ctx *FmtCtx) {
	ctx.WriteString("create account ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	ctx.WriteString(node.Name)
	node.AuthOption.Format(ctx)
	node.StatusOption.Format(ctx)
	node.Comment.Format(ctx)
}

func (node *CreateAccount) GetStatementType() string { return "Create Account" }
func (node *CreateAccount) GetQueryType() string     { return QueryTypeDCL }

func (node CreateAccount) TypeName() string { return "tree.CreateAccount" }

func (node *CreateAccount) reset() {
	*node = CreateAccount{}
}

func (node *CreateAccount) Free() {
	reuse.Free[CreateAccount](node, nil)
}

type AccountAuthOption struct {
	Equal          string
	AdminName      string
	IdentifiedType AccountIdentified
}

func NewAccountAuthOption(e string, an string, it AccountIdentified) *AccountAuthOption {
	ao := reuse.Alloc[AccountAuthOption](nil)
	ao.Equal = e
	ao.AdminName = an
	ao.IdentifiedType = it
	return ao
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

func (node AccountAuthOption) TypeName() string { return "tree.AccountAuthOption" }

func (node *AccountAuthOption) reset() {
	*node = AccountAuthOption{}
}

func (node *AccountAuthOption) Free() {
	reuse.Free[AccountAuthOption](node, nil)
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

func NewAccountIdentified(t AccountIdentifiedOption, s string) *AccountIdentified {
	// ai := reuse.Alloc[AccountIdentified](nil)
	ai := new(AccountIdentified)
	ai.Typ = t
	ai.Str = s
	return ai
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

func (node AccountIdentified) TypeName() string { return "tree.AccountIdentified" }

func (node *AccountIdentified) reset() {
	*node = AccountIdentified{}
}

func (node *AccountIdentified) Free() {
	// reuse.Free[AccountIdentified](node, nil)
}

type AccountStatusOption int

const (
	AccountStatusOpen AccountStatusOption = iota
	AccountStatusSuspend
	AccountStatusRestricted
)

func (aso AccountStatusOption) String() string {
	switch aso {
	case AccountStatusOpen:
		return "open"
	case AccountStatusSuspend:
		return "suspend"
	case AccountStatusRestricted:
		return "restricted"
	default:
		return "open"
	}
}

type AccountStatus struct {
	Exist  bool
	Option AccountStatusOption
}

func NewAccountStatus() *AccountStatus {
	as := reuse.Alloc[AccountStatus](nil)
	return as
}

func (node *AccountStatus) Format(ctx *FmtCtx) {
	if node.Exist {
		switch node.Option {
		case AccountStatusOpen:
			ctx.WriteString(" open")
		case AccountStatusSuspend:
			ctx.WriteString(" suspend")
		case AccountStatusRestricted:
			ctx.WriteString(" restricted")
		}
	}
}

func (node AccountStatus) TypeName() string { return "tree.AccountStatus" }

func (node *AccountStatus) reset() {
	*node = AccountStatus{}
}

func (node *AccountStatus) Free() {
	reuse.Free[AccountStatus](node, nil)
}

type AccountComment struct {
	Exist   bool
	Comment string
}

func NewAccountComment() *AccountComment {
	ac := reuse.Alloc[AccountComment](nil)
	return ac
}

func (node *AccountComment) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(" comment ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Comment))
	}
}

func (node AccountComment) TypeName() string { return "tree.AccountComment" }

func (node *AccountComment) reset() {
	*node = AccountComment{}
}

func (node *AccountComment) Free() {
	reuse.Free[AccountComment](node, nil)
}

type AccountCommentOrAttribute struct {
	Exist     bool
	IsComment bool
	Str       string
}

func NewAccountCommentOrAttribute(e bool, c bool, s string) *AccountCommentOrAttribute {
	// aca := reuse.Alloc[AccountCommentOrAttribute](nil)
	aca := new(AccountCommentOrAttribute)
	aca.Exist = e
	aca.IsComment = c
	aca.Str = s
	return aca
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

func (node AccountCommentOrAttribute) TypeName() string { return "tree.AccountCommentOrAttribute" }

func (node *AccountCommentOrAttribute) reset() {
	*node = AccountCommentOrAttribute{}
}

func (node *AccountCommentOrAttribute) Free() {
	// reuse.Free[AccountCommentOrAttribute](node, nil)
}

type CreatePublication struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	Database    Identifier
	AccountsSet *AccountsSetOption
	Comment     string
}

func NewCreatePublication(ife bool, n Identifier, db Identifier, as *AccountsSetOption, c string) *CreatePublication {
	cp := reuse.Alloc[CreatePublication](nil)
	cp.IfNotExists = ife
	cp.Name = n
	cp.Database = db
	cp.AccountsSet = as
	cp.Comment = c
	return cp
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

func (node CreatePublication) TypeName() string { return "tree.CreatePublication" }

func (node *CreatePublication) reset() {
	if node.AccountsSet != nil {
		node.AccountsSet.Free()
	}
	*node = CreatePublication{}
}

func (node *CreatePublication) Free() {
	reuse.Free[CreatePublication](node, nil)
}

type AttributeVisable struct {
	columnAttributeImpl
	Is bool // true NULL (default); false NOT NULL
}

func (node *AttributeVisable) Format(ctx *FmtCtx) {
	if node.Is {
		ctx.WriteString("visible")
	} else {
		ctx.WriteString("not visible")
	}
}

func (node AttributeVisable) TypeName() string { return "tree.AttributeVisable" }

func (node *AttributeVisable) reset() {
	*node = AttributeVisable{}
}

func (node *AttributeVisable) Free() {
	reuse.Free[AttributeVisable](node, nil)
}

func NewAttributeVisable(b bool) *AttributeVisable {
	av := reuse.Alloc[AttributeVisable](nil)
	av.Is = b
	return av
}
