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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

type RevokeType int

const (
	RevokeTypePrivilege RevokeType = iota
	RevokeTypeRole
)

type Revoke struct {
	statementImpl
	Typ             RevokeType
	RevokePrivilege RevokePrivilege
	RevokeRole      RevokeRole
}

func (node *Revoke) Format(ctx *FmtCtx) {
	switch node.Typ {
	case RevokeTypePrivilege:
		node.RevokePrivilege.Format(ctx)
	case RevokeTypeRole:
		node.RevokeRole.Format(ctx)
	}
}

type RevokePrivilege struct {
	statementImpl
	IfExists   bool
	Privileges []*Privilege
	ObjType    ObjectType
	Level      *PrivilegeLevel
	Roles      []*Role
}

func (node *RevokePrivilege) Format(ctx *FmtCtx) {
	ctx.WriteString("revoke")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}

	if node.Privileges != nil {
		prefix := " "
		for _, p := range node.Privileges {
			ctx.WriteString(prefix)
			p.Format(ctx)
			prefix = ", "
		}
	}
	ctx.WriteString(" on")
	if node.ObjType != OBJECT_TYPE_NONE {
		ctx.WriteByte(' ')
		ctx.WriteString(node.ObjType.String())
	}
	if node.Level != nil {
		ctx.WriteByte(' ')
		node.Level.Format(ctx)
	}

	if node.Roles != nil {
		ctx.WriteString(" from")
		prefix := " "
		for _, r := range node.Roles {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
}

func NewRevoke() *Revoke {
	return &Revoke{}
}

type RevokeRole struct {
	statementImpl
	IfExists bool
	Roles    []*Role
	Users    []*User
}

func (node *RevokeRole) Format(ctx *FmtCtx) {
	ctx.WriteString("revoke")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	if node.Roles != nil {
		prefix := " "
		for _, r := range node.Roles {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	if node.Users != nil {
		ctx.WriteString(" from")
		prefix := " "
		for _, r := range node.Users {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
}

type PrivilegeLevel struct {
	NodeFormatter
	Level       PrivilegeLevelType
	DbName      string
	TabName     string
	RoutineName string
}

func (node *PrivilegeLevel) Format(ctx *FmtCtx) {
	switch node.Level {
	case PRIVILEGE_LEVEL_TYPE_STAR:
		ctx.WriteString("*")
	case PRIVILEGE_LEVEL_TYPE_STAR_STAR:
		ctx.WriteString("*.*")
	case PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
		ctx.WriteString(fmt.Sprintf("%s.*", node.DbName))
	case PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
		ctx.WriteString(fmt.Sprintf("%s.%s", node.DbName, node.TabName))
	case PRIVILEGE_LEVEL_TYPE_TABLE:
		ctx.WriteString(node.TabName)
	}
}

func (node *PrivilegeLevel) String() string {
	fmtCtx := NewFmtCtx(dialect.MYSQL)
	node.Format(fmtCtx)
	return fmtCtx.String()
}

func NewPrivilegeLevel(l PrivilegeLevelType, d, t, r string) *PrivilegeLevel {
	return &PrivilegeLevel{
		Level:       l,
		DbName:      d,
		TabName:     t,
		RoutineName: r,
	}
}

type PrivilegeLevelType int

const (
	PRIVILEGE_LEVEL_TYPE_STAR           PrivilegeLevelType = iota //*
	PRIVILEGE_LEVEL_TYPE_STAR_STAR                                //*.*
	PRIVILEGE_LEVEL_TYPE_DATABASE                                 //db_name
	PRIVILEGE_LEVEL_TYPE_DATABASE_STAR                            //db_name.*
	PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE                           //db_name.tbl_name
	PRIVILEGE_LEVEL_TYPE_TABLE                                    //tbl_name
	PRIVILEGE_LEVEL_TYPE_COLUMN                                   // (x,x)
	PRIVILEGE_LEVEL_TYPE_STORED_ROUTINE                           //procedure
	PRIVILEGE_LEVEL_TYPE_PROXY
	PRIVILEGE_LEVEL_TYPE_ROUTINE
)

type Privilege struct {
	NodeFormatter
	Type       PrivilegeType
	ColumnList []*UnresolvedName
}

func (node *Privilege) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Type.ToString())
	if node.ColumnList != nil {
		prefix := "("
		for _, c := range node.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

func NewPrivilege(t PrivilegeType, c []*UnresolvedName) *Privilege {
	return &Privilege{
		Type:       t,
		ColumnList: c,
	}
}

type ObjectType int

func (node *ObjectType) String() string {
	switch *node {
	case OBJECT_TYPE_TABLE:
		return "table"
	case OBJECT_TYPE_FUNCTION:
		return "function"
	case OBJECT_TYPE_PROCEDURE:
		return "procedure"
	case OBJECT_TYPE_ACCOUNT:
		return "account"
	case OBJECT_TYPE_DATABASE:
		return "database"
	default:
		return "Unknown ObjectType"
	}
}

const (
	OBJECT_TYPE_NONE ObjectType = iota
	OBJECT_TYPE_TABLE
	OBJECT_TYPE_DATABASE
	OBJECT_TYPE_FUNCTION
	OBJECT_TYPE_PROCEDURE
	OBJECT_TYPE_VIEW
	OBJECT_TYPE_ACCOUNT
)

type PrivilegeType int

func (node *PrivilegeType) ToString() string {
	switch *node {
	case PRIVILEGE_TYPE_STATIC_ALL:
		return "all"
	case PRIVILEGE_TYPE_STATIC_ALTER:
		return "alter"
	case PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE:
		return "alter routine"
	case PRIVILEGE_TYPE_STATIC_CREATE:
		return "create"
	case PRIVILEGE_TYPE_STATIC_CREATE_ROLE:
		return "create role"
	case PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE:
		return "create routine"
	case PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE:
		return "create tablespace"
	case PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES:
		return "temporary tables"
	case PRIVILEGE_TYPE_STATIC_CREATE_USER:
		return "create user"
	case PRIVILEGE_TYPE_STATIC_CREATE_VIEW:
		return "create view"
	case PRIVILEGE_TYPE_STATIC_DELETE:
		return "delete"
	case PRIVILEGE_TYPE_STATIC_DROP:
		return "drop"
	case PRIVILEGE_TYPE_STATIC_DROP_ROLE:
		return "drop role"
	case PRIVILEGE_TYPE_STATIC_EVENT:
		return "event"
	case PRIVILEGE_TYPE_STATIC_EXECUTE:
		return "execute"
	case PRIVILEGE_TYPE_STATIC_FILE:
		return "file"
	case PRIVILEGE_TYPE_STATIC_GRANT_OPTION:
		return "grant option"
	case PRIVILEGE_TYPE_STATIC_INDEX:
		return "index"
	case PRIVILEGE_TYPE_STATIC_INSERT:
		return "insert"
	case PRIVILEGE_TYPE_STATIC_LOCK_TABLES:
		return "lock tables"
	case PRIVILEGE_TYPE_STATIC_PROCESS:
		return "process"
	case PRIVILEGE_TYPE_STATIC_PROXY:
		return "proxy"
	case PRIVILEGE_TYPE_STATIC_REFERENCES:
		return "reference"
	case PRIVILEGE_TYPE_STATIC_RELOAD:
		return "reload"
	case PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT:
		return "replication client"
	case PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE:
		return "replication slave"
	case PRIVILEGE_TYPE_STATIC_SELECT:
		return "select"
	case PRIVILEGE_TYPE_STATIC_SHOW_DATABASES:
		return "show databases"
	case PRIVILEGE_TYPE_STATIC_SHOW_VIEW:
		return "show view"
	case PRIVILEGE_TYPE_STATIC_SHUTDOWN:
		return "shutdown"
	case PRIVILEGE_TYPE_STATIC_SUPER:
		return "super"
	case PRIVILEGE_TYPE_STATIC_TRIGGER:
		return "trigger"
	case PRIVILEGE_TYPE_STATIC_UPDATE:
		return "update"
	case PRIVILEGE_TYPE_STATIC_USAGE:
		return "usage"
	case PRIVILEGE_TYPE_STATIC_CONNECT:
		return "connect"
	case PRIVILEGE_TYPE_STATIC_OWNERSHIP:
		return "ownership"
	case PRIVILEGE_TYPE_STATIC_MANAGE_GRANTS:
		return "manage"
	default:
		return "Unknown PrivilegeType"
	}
}

/*
*
From: https://dev.mysql.com/doc/refman/8.0/en/grant.html
*/
const (
	PRIVILEGE_TYPE_STATIC_ALL PrivilegeType = iota //Grant all privileges at specified access level except GRANT OPTION and PROXY.
	PRIVILEGE_TYPE_STATIC_CREATE_ACCOUNT
	PRIVILEGE_TYPE_STATIC_DROP_ACCOUNT
	PRIVILEGE_TYPE_STATIC_ALTER_ACCOUNT
	PRIVILEGE_TYPE_STATIC_CREATE_USER //Enable use of CREATE USER, DROP USER, RENAME USER, and REVOKE ALL PRIVILEGES. Level: Global.
	PRIVILEGE_TYPE_STATIC_DROP_USER
	PRIVILEGE_TYPE_STATIC_ALTER_USER
	PRIVILEGE_TYPE_STATIC_CREATE_ROLE //Enable role creation. Level: Global.
	PRIVILEGE_TYPE_STATIC_DROP_ROLE   //Enable roles to be dropped. Level: Global.
	PRIVILEGE_TYPE_STATIC_ALTER_ROLE
	PRIVILEGE_TYPE_STATIC_CREATE_DATABASE
	PRIVILEGE_TYPE_STATIC_DROP_DATABASE
	PRIVILEGE_TYPE_STATIC_SHOW_DATABASES //Enable SHOW DATABASES to show all databases. Level: Global.
	PRIVILEGE_TYPE_STATIC_CONNECT
	PRIVILEGE_TYPE_STATIC_MANAGE_GRANTS
	PRIVILEGE_TYPE_STATIC_OWNERSHIP
	PRIVILEGE_TYPE_STATIC_SHOW_TABLES
	PRIVILEGE_TYPE_STATIC_CREATE_TABLE
	PRIVILEGE_TYPE_STATIC_DROP_TABLE
	PRIVILEGE_TYPE_STATIC_DROP_VIEW
	PRIVILEGE_TYPE_STATIC_ALTER_TABLE
	PRIVILEGE_TYPE_STATIC_ALTER_VIEW
	PRIVILEGE_TYPE_STATIC_SELECT     //Enable use of SELECT. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_INSERT     //Enable use of INSERT. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_UPDATE     //Enable use of UPDATE. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_DELETE     //Enable use of DELETE. Level: Global, database, table.
	PRIVILEGE_TYPE_STATIC_REFERENCES //Enable foreign key creation. Levels: Global, database, table, column.
	PRIVILEGE_TYPE_STATIC_INDEX      //Enable indexes to be created or dropped. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_EXECUTE    //Enable the user to execute stored routines. Levels: Global, database, routine.
	PRIVILEGE_TYPE_STATIC_VALUES     //Enable use of VALUES. Levels: Global, database, table.

	PRIVILEGE_TYPE_STATIC_ALTER
	PRIVILEGE_TYPE_STATIC_CREATE
	PRIVILEGE_TYPE_STATIC_DROP
	PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE           //Enable stored routines to be altered or dropped. Levels: Global, database, routine.
	PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE          //Enable stored routine creation. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE       //Enable tablespaces and log file groups to be created, altered, or dropped. Level: Global.
	PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES //Enable use of CREATE TEMPORARY TABLE. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_CREATE_VIEW             //Enable views to be created or altered. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_EVENT                   //Enable use of events for the Event Scheduler. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_FILE                    //Enable the user to cause the server to read or write files. Level: Global.
	PRIVILEGE_TYPE_STATIC_GRANT_OPTION            //Enable privileges to be granted to or removed from other accounts. Levels: Global, database, table, routine, proxy.
	PRIVILEGE_TYPE_STATIC_LOCK_TABLES             //Enable use of LOCK TABLES on tables for which you have the SELECT privilege. Levels: Global, database.
	PRIVILEGE_TYPE_STATIC_PROCESS                 //Enable the user to see all processes with SHOW PROCESSLIST. Level: Global.
	PRIVILEGE_TYPE_STATIC_PROXY                   //Enable user proxying. Level: From user to user.
	PRIVILEGE_TYPE_STATIC_RELOAD                  //Enable use of FLUSH operations. Level: Global.
	PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT      //Enable the user to ask where source or replica servers are. Level: Global.
	PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE       //Enable replicas to read binary log events from the source. Level: Global.
	PRIVILEGE_TYPE_STATIC_SHOW_VIEW               //Enable use of SHOW CREATE VIEW. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_SHUTDOWN                //Enable use of mysqladmin shutdown. Level: Global.
	PRIVILEGE_TYPE_STATIC_SUPER                   //Enable use of other administrative operations such as CHANGE REPLICATION SOURCE TO, CHANGE MASTER TO, KILL, PURGE BINARY LOGS, SET GLOBAL, and mysqladmin debug command. Level: Global.
	PRIVILEGE_TYPE_STATIC_TRIGGER                 //Enable trigger operations. Levels: Global, database, table.
	PRIVILEGE_TYPE_STATIC_USAGE                   //Synonym for “no privileges”
	PRIVILEGE_TYPE_
	PRIVILEGE_TYPE_DYNAMIC_APPLICATION_PASSWORD_ADMIN //Enable dual password administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_AUDIT_ADMIN                //Enable audit log configuration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BACKUP_ADMIN               //Enable backup administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BINLOG_ADMIN               //Enable binary log control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_BINLOG_ENCRYPTION_ADMIN    //Enable activation and deactivation of binary log encryption. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_CLONE_ADMIN                //Enable clone administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_CONNECTION_ADMIN           //Enable connection limit/restriction control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_ENCRYPTION_KEY_ADMIN       //Enable InnoDB key rotation. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_ADMIN             //Enable firewall rule administration, any user. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_EXEMPT            //Exempt user from firewall restrictions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FIREWALL_USER              //Enable firewall rule administration, self. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_OPTIMIZER_COSTS      //Enable optimizer cost reloading. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_STATUS               //Enable status indicator flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_TABLES               //Enable table flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_FLUSH_USER_RESOURCES       //Enable user-resource flushing. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_GROUP_REPLICATION_ADMIN    //Enable Group Replication control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_INNODB_REDO_LOG_Enable     //Enable or disable redo logging. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_INNODB_REDO_LOG_ARCHIVE    //Enable redo log archiving administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_NDB_STORED_USER            //Enable sharing of user or role between SQL nodes (NDB Cluster). Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_PERSIST_RO_VARIABLES_ADMIN //Enable persisting read-only system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_REPLICATION_APPLIER        //Act as the PRIVILEGE_CHECKS_USER for a replication channel. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_REPLICATION_SLAVE_ADMIN    //Enable regular replication control. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_RESOURCE_GROUP_ADMIN       //Enable resource group administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_RESOURCE_GROUP_USER        //Enable resource group administration. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_ROLE_ADMIN                 //Enable roles to be granted or revoked, use of WITH ADMIN OPTION. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SESSION_VARIABLES_ADMIN    //Enable setting restricted session system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SET_USER_ID                //Enable setting non-self DEFINER values. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SHOW_ROUTINE               //Enable access to stored routine definitions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SYSTEM_USER                //Designate account as system account. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_SYSTEM_VARIABLES_ADMIN     //Enable modifying or persisting global system variables. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_TABLE_ENCRYPTION_ADMIN     //Enable overriding default encryption settings. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_VERSION_TOKEN_ADMIN        //Enable use of Version Tokens functions. Level: Global.
	PRIVILEGE_TYPE_DYNAMIC_XA_RECOVER_ADMIN           //Enable XA RECOVER execution. Level: Global.
)
