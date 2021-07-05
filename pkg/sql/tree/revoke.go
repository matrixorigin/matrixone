package tree

type PrivilegeType int

/**
From: https://dev.mysql.com/doc/refman/8.0/en/grant.html
 */
const (
	PRIVILEGE_TYPE_STATIC_ALL 	PrivilegeType = iota//Grant all privileges at specified access level except GRANT OPTION and PROXY.
	PRIVILEGE_TYPE_STATIC_ALTER	//Enable use of ALTER TABLE. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE	//Enable stored routines to be altered or dropped. Levels: Global, database, routine.
	PRIVILEGE_TYPE_STATIC_CREATE	//Enable database and table creation. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_CREATE_ROLE	//Enable role creation. Level: Global.
	PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE	//Enable stored routine creation. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE	//Enable tablespaces and log file groups to be created, altered, or dropped. Level: Global.
	PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES	//Enable use of CREATE TEMPORARY TABLE. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_CREATE_USER	//Enable use of CREATE USER, DROP USER, RENAME USER, and REVOKE ALL PRIVILEGES. Level: Global.
	PRIVILEGE_TYPE_STATIC_CREATE_VIEW	//Enable views to be created or altered. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_DELETE	//Enable use of DELETE. Level: Global, database, table.
	PRIVILEGE_TYPE_STATIC_DROP	//Enable databases, tables, and views to be dropped. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_DROP_ROLE	//Enable roles to be dropped. Level: Global.
	PRIVILEGE_TYPE_STATIC_EVENT	//Enable use of events for the Event Scheduler. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_EXECUTE	//Enable the user to execute stored routines. Levels: Global, database, routine.
	PRIVILEGE_TYPE_STATIC_FILE	//Enable the user to cause the server to read or write files. Level: Global.
	PRIVILEGE_TYPE_STATIC_GRANT_OPTION	//Enable privileges to be granted to or removed from other accounts. Levels: Global, database, table, routine, proxy.
	PRIVILEGE_TYPE_STATIC_INDEX	//Enable indexes to be created or dropped. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_INSERT	//Enable use of INSERT. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_LOCK_TABLES	//Enable use of LOCK TABLES on tables for which you have the SELECT privilege. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_PROCESS	//Enable the user to see all processes with SHOW PROCESSLIST. Level: Global.
	PRIVILEGE_TYPE_STATIC_PROXY	//Enable user proxying. Level: From user to user.
	PRIVILEGE_TYPE_STATIC_REFERENCES	//Enable foreign key creation. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_RELOAD	//Enable use of FLUSH operations. Level: Global.
	PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT	//Enable the user to ask where source or replica servers are. Level: Global.
	PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE	//Enable replicas to read binary log events from the source. Level: Global.
	PRIVILEGE_TYPE_STATIC_SELECT	//Enable use of SELECT. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_SHOW_DATABASES	//Enable SHOW DATABASES to show all databases. Level: Global.
	PRIVILEGE_TYPE_STATIC_SHOW_VIEW	//Enable use of SHOW CREATE VIEW. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_SHUTDOWN	//Enable use of mysqladmin shutdown. Level: Global.
	PRIVILEGE_TYPE_STATIC_SUPER	//Enable use of other administrative operations such as CHANGE REPLICATION SOURCE TO, CHANGE MASTER TO, KILL, PURGE BINARY LOGS, SET GLOBAL, and mysqladmin debug command. Level: Global.
	PRIVILEGE_TYPE_STATIC_TRIGGER	//Enable trigger operations. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_UPDATE	//Enable use of UPDATE. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_USAGE	//Synonym for “no privileges”
	PRIVILEGE_TYPE_
	PRIVILEGE_TYPE_DYNAMIC_APPLICATION_PASSWORD_ADMIN	//Enable dual password administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_AUDIT_ADMIN	//Enable audit log configuration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BACKUP_ADMIN	//Enable backup administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BINLOG_ADMIN	//Enable binary log control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BINLOG_ENCRYPTION_ADMIN	//Enable activation and deactivation of binary log encryption. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_CLONE_ADMIN	//Enable clone administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_CONNECTION_ADMIN	//Enable connection limit/restriction control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_ENCRYPTION_KEY_ADMIN	//Enable InnoDB key rotation. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_ADMIN	//Enable firewall rule administration, any user. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_EXEMPT	//Exempt user from firewall restrictions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_USER	//Enable firewall rule administration, self. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_OPTIMIZER_COSTS	//Enable optimizer cost reloading. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_STATUS	//Enable status indicator flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_TABLES	//Enable table flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_USER_RESOURCES	//Enable user-resource flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_GROUP_REPLICATION_ADMIN	//Enable Group Replication control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_INNODB_REDO_LOG_Enable	//Enable or disable redo logging. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_INNODB_REDO_LOG_ARCHIVE	//Enable redo log archiving administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_NDB_STORED_USER	//Enable sharing of user or role between SQL nodes (NDB Cluster). Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_PERSIST_RO_VARIABLES_ADMIN	//Enable persisting read-only system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_REPLICATION_APPLIER	//Act as the PRIVILEGE_CHECKS_USER for a replication channel. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_REPLICATION_SLAVE_ADMIN	//Enable regular replication control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_RESOURCE_GROUP_ADMIN	//Enable resource group administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_RESOURCE_GROUP_USER	//Enable resource group administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_ROLE_ADMIN	//Enable roles to be granted or revoked, use of WITH ADMIN OPTION. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SESSION_VARIABLES_ADMIN	//Enable setting restricted session system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SET_USER_ID	//Enable setting non-self DEFINER values. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SHOW_ROUTINE	//Enable access to stored routine definitions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SYSTEM_USER	//Designate account as system account. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SYSTEM_VARIABLES_ADMIN	//Enable modifying or persisting global system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_TABLE_ENCRYPTION_ADMIN	//Enable overriding default encryption settings. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_VERSION_TOKEN_ADMIN	//Enable use of Version Tokens functions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_XA_RECOVER_ADMIN	//Enable XA RECOVER execution. Level: Global.
)

type PrivilegeLevelType int

const (
	PRIVILEGE_LEVEL_TYPE_GLOBAL         PrivilegeLevelType = iota //*.*
	PRIVILEGE_LEVEL_TYPE_DATABASE                                 //db_name.*
	PRIVILEGE_LEVEL_TYPE_TABLE                                    //db_name.tbl_name
	PRIVILEGE_LEVEL_TYPE_COLUMN                                   // (x,x)
	PRIVILEGE_LEVEL_TYPE_STORED_ROUTINE                           //procedure
	PRIVILEGE_LEVEL_TYPE_PROXY
)

type ObjectType int

const(
	OBJECT_TYPE_NONE ObjectType = iota
	OBJECT_TYPE_TABLE
	OBJECT_TYPE_FUNCTION
	OBJECT_TYPE_PROCEDURE
)

type Privilege struct {
	NodePrinter
	Type PrivilegeType
	ColumnList []*UnresolvedName
}

func NewPrivilege(t PrivilegeType,c []*UnresolvedName) *Privilege {
	return &Privilege{
		Type:        t,
		ColumnList:  c,
	}
}

type PrivilegeLevel struct {
	NodePrinter
	Level PrivilegeLevelType
	DbName string
	TabName string
	RoutineName string
}

func NewPrivilegeLevel(l PrivilegeLevelType,d,t,r string) *PrivilegeLevel{
	return &PrivilegeLevel{
		Level:       l,
		DbName:      d,
		TabName:     t,
		RoutineName: r,
	}
}

type Revoke struct {
	statementImpl
	IsRevokeRole bool
	RolesInRevokeRole []*Role
	Privileges []*Privilege
	ObjType ObjectType
	Level *PrivilegeLevel
	Users []*User
	Roles []*Role
}

func NewRevoke(irr bool, rirr []*Role, p []*Privilege, o ObjectType, l *PrivilegeLevel, u []*User, r []*Role) *Revoke {
	return &Revoke{
		IsRevokeRole: irr,
		RolesInRevokeRole: rirr,
		Privileges:    p,
		ObjType:       o,
		Level:         l,
		Users:         u,
		Roles:         r,
	}
}
