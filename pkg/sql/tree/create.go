package tree

type CreateOption interface {
	NodePrinter
}

type createOptionImpl struct {
	CreateOption
}

type CreateOptionDefault struct {
	createOptionImpl
}

type CreateOptionCharset struct {
	createOptionImpl
	Charset string
}

func NewCreateOptionCharset(c string)*CreateOptionCharset{
	return &CreateOptionCharset{
		Charset:          c,
	}
}

type CreateOptionCollate struct {
	createOptionImpl
	Collate string
}

func NewCreateOptionCollate(c string)*CreateOptionCollate{
	return &CreateOptionCollate{
		Collate: c,
	}
}

type CreateOptionEncryption struct {
	createOptionImpl
	Encrypt string
}

func NewCreateOptionEncryption(e string)*CreateOptionEncryption{
	return &CreateOptionEncryption{
		Encrypt: e,
	}
}

type CreateDatabase struct {
	statementImpl
	IfNotExists     bool
	Name            Identifier
	CreateOptions []CreateOption
}

func NewCreateDatabase(ine bool,name Identifier,opts []CreateOption)*CreateDatabase{
	return &CreateDatabase{
		IfNotExists:   ine,
		Name:          name,
		CreateOptions: opts,
	}
}

type CreateTable struct {
	statementImpl
	IfNotExists      bool
	Table            TableName
	Defs     TableDefs
	Options  []TableOption
	PartitionOption *PartitionOption
}

type TableDef interface {
	NodePrinter
}

type tableDefImpl struct {
	TableDef
}

//the list of table definitions
type TableDefs []TableDef

type ColumnTableDef struct {
	tableDefImpl
	Name     *UnresolvedName
	Type     ResolvableTypeReference
	Attributes []ColumnAttribute
}

func NewColumnTableDef(n *UnresolvedName,t ResolvableTypeReference,a []ColumnAttribute)*ColumnTableDef{
	return &ColumnTableDef{
		Name:         n,
		Type:         t,
		Attributes:   a,
	}
}

//column attribute
type ColumnAttribute interface {
	NodePrinter
}

type columnAttributeImpl struct {
	ColumnAttribute
}

type AttributeNull struct {
	columnAttributeImpl
	Is bool //true NULL (default); false NOT NULL
}

func NewAttributeNull(b bool)*AttributeNull{
	return &AttributeNull{
		Is:              b,
	}
}

type AttributeDefault struct {
	columnAttributeImpl
	Expr Expr
}

func NewAttributeDefault(e Expr)*AttributeDefault{
	return &AttributeDefault{
		Expr: e,
	}
}

type AttributeAutoIncrement struct {
	columnAttributeImpl
}

func NewAttributeAutoIncrement()*AttributeAutoIncrement{
	return &AttributeAutoIncrement{}
}

type AttributeUniqueKey struct {
	columnAttributeImpl
}

func NewAttributeUniqueKey()*AttributeUniqueKey{
	return &AttributeUniqueKey{}
}

type AttributePrimaryKey struct {
	columnAttributeImpl
}

func NewAttributePrimaryKey()*AttributePrimaryKey{
	return &AttributePrimaryKey{}
}

type AttributeComment struct {
	columnAttributeImpl
	CMT Expr
}

func NewAttributeComment(c Expr)*AttributeComment{
	return &AttributeComment{
		CMT: c,
	}
}

type AttributeCollate struct {
	columnAttributeImpl
	Collate string
}

func NewAttributeCollate(c string)*AttributeCollate{
	return &AttributeCollate{
		Collate:             c,
	}
}

type AttributeColumnFormat struct {
	columnAttributeImpl
	Format string
}

func NewAttributeColumnFormat(f string) *AttributeColumnFormat {
	return &AttributeColumnFormat{
		Format:              f,
	}
}

type AttributeStorage struct {
	columnAttributeImpl
	Storage string
}

func NewAttributeStorage(s string)*AttributeStorage{
	return &AttributeStorage{
		Storage:             s,
	}
}

type AttributeCheckConstraint struct {
	columnAttributeImpl
	Name string
	Expr Expr
	Enforced bool
}

func NewAttributeCheck(e Expr, f bool, n string) *AttributeCheckConstraint {
	return &AttributeCheckConstraint{
		Name: n,
		Expr:                e,
		Enforced:            f,
	}
}

type AttributeGeneratedAlways struct {
	columnAttributeImpl
	Expr Expr
	Stored bool
}

func NewAttributeGeneratedAlways(e Expr,s bool)*AttributeGeneratedAlways{
	return &AttributeGeneratedAlways{
		Expr:                e,
		Stored:              s,
	}
}

type KeyPart struct {
	columnAttributeImpl
	ColName *UnresolvedName
	Length int
	Expr Expr
}

func NewKeyPart(c *UnresolvedName,l int,e Expr)*KeyPart{
	return &KeyPart{
		ColName:             c,
		Length:              l,
		Expr:                e,
	}
}

//in reference definition
type MatchType int

const (
	MATCH_INVALID MatchType = iota
	MATCH_FULL
	MATCH_PARTIAL
	MATCH_SIMPLE
)

type ReferenceOptionType int

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
	KeyParts []*KeyPart
	Match MatchType
	OnDelete ReferenceOptionType
	OnUpdate ReferenceOptionType
}

func NewAttributeReference(t *TableName,kps []*KeyPart,m MatchType,
			od ReferenceOptionType,ou ReferenceOptionType)*AttributeReference{
	return &AttributeReference{
		TableName:           t,
		KeyParts:            kps,
		Match:               m,
		OnDelete:            od,
		OnUpdate:            ou,
	}
}

type AttributeAutoRandom struct {
	columnAttributeImpl
	BitLength int
}

func NewAttributeAutoRandom(b int)*AttributeAutoRandom{
	return &AttributeAutoRandom{
		BitLength:           b,
	}
}

type AttributeOnUpdate struct {
	columnAttributeImpl
	Expr Expr
}

func NewAttributeOnUpdate(e Expr)*AttributeOnUpdate{
	return &AttributeOnUpdate{
		Expr:                e,
	}
}

type IndexTableDef interface {
	TableDef
}

type indexTableDefImpl struct {
	IndexTableDef
}

type IndexType int

const (
	INDEX_TYPE_INVALID IndexType = iota
	INDEX_TYPE_BTREE
	INDEX_TYPE_HASH
	INDEX_TYPE_RTREE
)

type VisibleType int

const (
	VISIBLE_TYPE_INVALID VisibleType = iota
	VISIBLE_TYPE_VISIBLE
	VISIBLE_TYPE_INVISIBLE
)

type IndexOption struct {
	NodePrinter
	KeyBlockSize uint64
	iType        IndexType
	ParserName   string
	Comment      string
	Visible		 VisibleType
	EngineAttribute string
	SecondaryEngineAttribute string
}

func NewIndexOption(k uint64,i IndexType,p string,c string,v VisibleType,e string,se string)*IndexOption{
	return &IndexOption{
		KeyBlockSize:             k,
		iType:                    i,
		ParserName:               p,
		Comment:                  c,
		Visible:                  v,
		EngineAttribute:          e,
		SecondaryEngineAttribute: se,
	}
}

type PrimaryKeyIndex struct {
	indexTableDefImpl
	KeyParts []*KeyPart
	Name string
	Empty bool
	IndexOption *IndexOption
}

func NewPrimaryKeyIndex(k []*KeyPart,n string,e bool,io *IndexOption)*PrimaryKeyIndex {
	return &PrimaryKeyIndex{
		KeyParts:            k,
		Name:                n,
		Empty:               e,
		IndexOption:		io,
	}
}

type Index struct {
	indexTableDefImpl
	KeyParts []*KeyPart
	Name string
	Empty bool
	IndexOption *IndexOption
}

func NewIndex(k []*KeyPart,n string,e bool,io *IndexOption)*Index {
	return &Index{
		KeyParts:            k,
		Name:                n,
		Empty:               e,
		IndexOption:		io,
	}
}

type UniqueIndex struct {
	indexTableDefImpl
	KeyParts []*KeyPart
	Name string
	Empty bool
	IndexOption *IndexOption
}

func NewUniqueIndex(k []*KeyPart,n string,e bool,io *IndexOption)*UniqueIndex {
	return &UniqueIndex{
		KeyParts:            k,
		Name:                n,
		Empty:               e,
		IndexOption:		io,
	}
}

type ForeignKey struct {
	indexTableDefImpl
	IfNotExists bool
	KeyParts []*KeyPart
	Name string
	Refer *AttributeReference
	Empty bool
}

func NewForeignKey(ine bool,k []*KeyPart,n string,r *AttributeReference,e bool)*ForeignKey {
	return &ForeignKey{
		IfNotExists: ine,
		KeyParts:            k,
		Name:                n,
		Refer: r,
		Empty:               e,
	}
}

type FullTextIndex struct {
	indexTableDefImpl
	KeyParts []*KeyPart
	Name string
	Empty bool
	IndexOption *IndexOption
}

func NewFullTextIndex(k []*KeyPart,n string,e bool,io *IndexOption)*FullTextIndex {
	return &FullTextIndex{
		KeyParts:            k,
		Name:                n,
		Empty:               e,
		IndexOption:		io,
	}
}

type CheckIndex struct {
	indexTableDefImpl
	Expr Expr
	Enforced bool
}

func NewCheckIndex(e Expr,en bool)*CheckIndex {
	return &CheckIndex{
		Expr: e,
		Enforced: en,
	}
}

type TableOption interface {
	NodePrinter
}

type tableOptionImpl struct {
	TableOption
}

type TableOptionEngine struct {
	tableOptionImpl
	Engine string
}

func NewTableOptionEngine(s string)*TableOptionEngine{
	return &TableOptionEngine{
		Engine:         	s,
	}
}

type TableOptionSecondaryEngine struct {
	tableOptionImpl
	Engine string
}

func NewTableOptionSecondaryEngine(s string)*TableOptionSecondaryEngine{
	return &TableOptionSecondaryEngine{
		Engine:         	s,
	}
}

type TableOptionSecondaryEngineNull struct {
	tableOptionImpl
}

func NewTableOptionSecondaryEngineNull()*TableOptionSecondaryEngineNull{
	return &TableOptionSecondaryEngineNull{}
}

type TableOptionCharset struct {
	tableOptionImpl
	Charset string
}

func NewTableOptionCharset(s string)*TableOptionCharset{
	return &TableOptionCharset{Charset: s}
}

type TableOptionCollate struct {
	tableOptionImpl
	Collate string
}

func NewTableOptionCollate(s string) *TableOptionCollate {
	return &TableOptionCollate{
		Collate:         s,
	}
}

type TableOptionAutoIncrement struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionAutoIncrement(v uint64)*TableOptionAutoIncrement  {
	return &TableOptionAutoIncrement{
		Value: v,
	}
}

type TableOptionComment struct {
	tableOptionImpl
	Comment string
}

func NewTableOptionComment(c string)*TableOptionComment{
	return &TableOptionComment{
		Comment: c,
	}
}

type TableOptionAvgRowLength struct {
	tableOptionImpl
	Length uint64
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

func NewTableOptionChecksum(v uint64)*TableOptionChecksum{
	return &TableOptionChecksum{
		Value:           v,
	}
}

type TableOptionCompression struct {
	tableOptionImpl
	Compression string
}

func NewTableOptionCompression(c string) *TableOptionCompression {
	return &TableOptionCompression{
		Compression:     c,
	}
}

type TableOptionConnection struct {
	tableOptionImpl
	Connection string
}

func NewTableOptionConnection(c string)*TableOptionConnection{
	return &TableOptionConnection{
		Connection: c,
	}
}

type TableOptionPassword struct {
	tableOptionImpl
	Password string
}

func NewTableOptionPassword(p string)*TableOptionPassword{
	return &TableOptionPassword{
		Password: p,
	}
}

type TableOptionKeyBlockSize struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionKeyBlockSize(v uint64)*TableOptionKeyBlockSize{
	return &TableOptionKeyBlockSize{
		Value: v,
	}
}

type TableOptionMaxRows struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionMaxRows(v uint64)*TableOptionMaxRows{
	return &TableOptionMaxRows{
		Value: v,
	}
}

type TableOptionMinRows struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionMinRows(v uint64)*TableOptionMinRows{
	return &TableOptionMinRows{
		Value: v,
	}
}

type TableOptionDelayKeyWrite struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionDelayKeyWrite(v uint64)*TableOptionDelayKeyWrite{
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

type TableOptionRowFormat struct {
	tableOptionImpl
	Value RowFormatType
}

func NewTableOptionRowFormat(v RowFormatType)*TableOptionRowFormat{
	return &TableOptionRowFormat{
		Value: v,
	}
}

type TableOptionStatsPersistent struct {
	tableOptionImpl
	//missing value
}

func NewTableOptionStatsPersistent()*TableOptionStatsPersistent{
	return &TableOptionStatsPersistent{}
}

type TableOptionStatsAutoRecalc struct {
	tableOptionImpl
	Value uint64
	Default bool //false -- see Value; true -- Value is useless
}

func NewTableOptionStatsAutoRecalc(v uint64,d bool)*TableOptionStatsAutoRecalc{
	return &TableOptionStatsAutoRecalc{
		Value: v,
		Default: d,
	}
}

type TableOptionPackKeys struct {
	tableOptionImpl
	//missing value
}

func NewTableOptionPackKeys()*TableOptionPackKeys{
	return &TableOptionPackKeys{}
}

type TableOptionTablespace struct {
	tableOptionImpl
	Name string
}

func NewTableOptionTablespace(n string) *TableOptionTablespace {
	return &TableOptionTablespace{Name: n}
}

type TableOptionDataDirectory struct {
	tableOptionImpl
	Dir string
}

func NewTableOptionDataDirectory(d string)*TableOptionDataDirectory{
	return &TableOptionDataDirectory{Dir: d}
}

type TableOptionIndexDirectory struct {
	tableOptionImpl
	Dir string
}

func NewTableOptionIndexDirectory(d string)*TableOptionIndexDirectory{
	return &TableOptionIndexDirectory{
		Dir:             d,
	}
}

type TableOptionStorageMedia struct {
	tableOptionImpl
	Media string
}

func NewTableOptionStorageMedia(m string)*TableOptionStorageMedia{
	return &TableOptionStorageMedia{Media: m}
}

type TableOptionStatsSamplePages struct {
	tableOptionImpl
	Value uint64
	Default bool //false -- see Value; true -- Value is useless
}

func NewTableOptionStatsSamplePages(v uint64,d bool)*TableOptionStatsSamplePages{
	return &TableOptionStatsSamplePages{
		Value:           v,
		Default:         d,
	}
}

type TableOptionUnion struct {
	tableOptionImpl
	Names []*TableName
}

func NewTableOptionUnion(n []*TableName)*TableOptionUnion{
	return &TableOptionUnion{Names: n}
}

type TableOptionEncryption struct {
	tableOptionImpl
	Encryption string
}

func NewTableOptionEncryption(e string)*TableOptionEncryption{
	return &TableOptionEncryption{Encryption: e}
}

type PartitionType interface {
	NodePrinter
}

type partitionTypeImpl struct {
	PartitionType
}

type HashType struct {
	partitionTypeImpl
	Linear bool
	Expr Expr
}

func NewHashType(l bool,e Expr)*HashType{
	return &HashType{
		Linear:            l,
		Expr:              e,
	}
}

type KeyType struct {
	partitionTypeImpl
	Linear bool
	ColumnList []*UnresolvedName
}

func NewKeyType(l bool,c []*UnresolvedName)*KeyType{
	return &KeyType{
		Linear:            l,
		ColumnList:        c,
	}
}

type RangeType struct {
	partitionTypeImpl
	Expr Expr
	ColumnList []*UnresolvedName
}

func NewRangeType(e Expr, c []*UnresolvedName) *RangeType {
	return &RangeType{
		Expr:              e,
		ColumnList:        c,
	}
}

type ListType struct {
	partitionTypeImpl
	Expr Expr
	ColumnList []*UnresolvedName
}

func NewListType(e Expr, c []*UnresolvedName) *ListType {
	return &ListType{
		Expr:              e,
		ColumnList:        c,
	}
}

type PartitionBy struct {
	PType PartitionType
	Num uint64
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
	NodePrinter
}

type valuesImpl struct {
	Values
}

type ValuesLessThan struct {
	valuesImpl
	ValueList Exprs
}

func NewValuesLessThan(vl Exprs)*ValuesLessThan{
	return &ValuesLessThan{
		ValueList:  vl,
	}
}

type ValuesIn struct {
	valuesImpl
	ValueList []Exprs
}

func NewValuesIn(vl []Exprs)*ValuesIn{
	return &ValuesIn{
		ValueList:  vl,
	}
}

type Partition struct {
	Name Identifier
	Values Values
	Options []TableOption
	Subs []*SubPartition
}

func NewPartition(n Identifier,v Values,o []TableOption,s []*SubPartition)*Partition{
	return &Partition{
		Name:    n,
		Values:  v,
		Options: o,
		Subs:    s,
	}
}

type SubPartition struct {
	Name Identifier
	Options []TableOption
}

func NewSubPartition(n Identifier,o []TableOption)*SubPartition{
	return &SubPartition{
		Name:    n,
		Options: o,
	}
}

type PartitionOption struct {
	PartBy PartitionBy
	SubPartBy *PartitionBy
	Partitions []*Partition
}

func NewPartitionOption(pb *PartitionBy,spb *PartitionBy,parts []*Partition)*PartitionOption  {
	return &PartitionOption{
		PartBy:     *pb,
		SubPartBy:  spb,
		Partitions: parts,
	}
}

type IndexCategory int

const (
	INDEX_CATEGORY_NONE IndexCategory = iota
	INDEX_CATEGORY_UNIQUE
	INDEX_CATEGORY_FULLTEXT
	INDEX_CATEGORY_SPATIAL
)

type CreateIndex struct {
	statementImpl
	Name Identifier
	Table TableName
	IndexCat IndexCategory
	IfNotExists bool
	KeyParts []*KeyPart
	IndexOption *IndexOption
	MiscOption []MiscOption
}

func NewCreateIndex(n Identifier, t TableName, ife bool, it IndexCategory, k []*KeyPart, i *IndexOption, m []MiscOption) *CreateIndex {
	return &CreateIndex{
		Name:          n,
		Table:         t,
		IfNotExists: ife,
		IndexCat: it,
		KeyParts:      k,
		IndexOption:   i,
		MiscOption:    m,
	}
}

type MiscOption interface {
	NodePrinter
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
	Roles []*Role
}

func NewCreateRole(ife bool,r []*Role) *CreateRole {
	return &CreateRole{
		IfNotExists:   ife,
		Roles:         r,
	}
}

type Role struct {
	NodePrinter
	UserName string
	HostName string
}

func NewRole(u,h string) *Role {
	return &Role{
		UserName:    u,
		HostName:    h,
	}
}

type User struct {
	NodePrinter
	Username string
	Hostname string
	AuthPlugin string
	AuthString string
}

func NewUser(u,h,ap,as string)*User{
	return &User{
		Username:    u,
		Hostname:    h,
		AuthPlugin:  ap,
		AuthString:  as,
	}
}

type TlsOption interface {
	NodePrinter
}

type tlsOptionImpl struct {
	TlsOption
}

type TlsOptionNone struct {
	tlsOptionImpl
}

type TlsOptionSSL struct {
	tlsOptionImpl
}

type TlsOptionX509 struct {
	tlsOptionImpl
}

type TlsOptionCipher struct {
	tlsOptionImpl
	Cipher string
}

type TlsOptionIssuer struct {
	tlsOptionImpl
	Issuer string
}

type TlsOptionSubject struct {
	tlsOptionImpl
	Subject string
}

type ResourceOption interface {
}

type resourceOptionImpl struct {
	ResourceOption
}

type ResourceOptionMaxQueriesPerHour struct {
	resourceOptionImpl
	Count int64
}

type ResourceOptionMaxUpdatesPerHour struct {
	resourceOptionImpl
	Count int64
}

type ResourceOptionMaxConnectionPerHour struct {
	resourceOptionImpl
	Count int64
}

type ResourceOptionMaxUserConnections struct {
	resourceOptionImpl
	Count int64
}

type UserMiscOption interface {
}

type userMiscOptionImpl struct {
	UserMiscOption
}

type UserMiscOptionPasswordExpireNone struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordExpireDefault struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordExpireNever struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordExpireInterval struct {
	userMiscOptionImpl
	Value int64
}

type UserMiscOptionPasswordHistoryDefault struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordHistoryCount struct {
	userMiscOptionImpl
	Value int
}

type UserMiscOptionPasswordReuseIntervalDefault struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordReuseIntervalCount struct {
	userMiscOptionImpl
	Value int
}

type UserMiscOptionPasswordRequireCurrentDefault struct {
	userMiscOptionImpl
}

type UserMiscOptionPasswordRequireCurrentOptional struct {
	userMiscOptionImpl
}

type UserMiscOptionFailedLoginAttempts struct {
	userMiscOptionImpl
	Value int
}

type UserMiscOptionPasswordLockTimeCount struct {
	userMiscOptionImpl
	Value int
}

type UserMiscOptionPasswordLockTimeUnbounded struct {
	userMiscOptionImpl
}

type UserMiscOptionAccountLock struct {
	userMiscOptionImpl
}

type UserMiscOptionAccountUnlock struct {
	userMiscOptionImpl
}

type CreateUser struct {
	statementImpl
	IfNotExists bool
	Users []*User
	Roles []*Role
	TlsOpts []TlsOption
	ResOpts []ResourceOption
	MiscOpts []UserMiscOption
}

func NewCreateUser(ife bool, u []*User, r []*Role, tls []TlsOption, res []ResourceOption, misc []UserMiscOption) *CreateUser {
	return &CreateUser{
		IfNotExists: ife,
		Users:         u,
		Roles:         r,
		TlsOpts:       tls,
		ResOpts:       res,
		MiscOpts:      misc,
	}
}