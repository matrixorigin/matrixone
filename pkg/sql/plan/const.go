package plan

const (
	/*
		https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html
		MySQL has hard limit of 4096 columns per table, but the effective maximum may be less for a given table.
	*/
	TableColumnCountLimit = 4096
)

const (
	/*
		Identifier Length Limits
		See MySQL: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	*/
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxTableNameLength is max length of table name identifier.
	MaxTableNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxForeignKeyIdentifierLen is max length of foreign key identifier.
	MaxForeignKeyIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)

const (
	// PrimaryKeyName defines primary key name.
	PrimaryKeyName = "PRIMARY"
)
