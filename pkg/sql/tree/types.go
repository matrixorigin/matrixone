package tree

import (
	"matrixone/pkg/client"
)

type InternalType uint8

//sql type
type T struct {
	InternalType InternalType
	//fractional seconds precision in TIME,DATETIME,TIMESTAMP
	Fsp int
}

var (
	TYPE_TINY      = &T{InternalType: InternalType(client.MYSQL_TYPE_TINY)}
	TYPE_SHORT     = &T{InternalType: InternalType(client.MYSQL_TYPE_SHORT)}
	TYPE_LONG      = &T{InternalType: InternalType(client.MYSQL_TYPE_LONG)}
	TYPE_FLOAT     = &T{InternalType: InternalType(client.MYSQL_TYPE_FLOAT)}
	TYPE_DOUBLE    = &T{InternalType: InternalType(client.MYSQL_TYPE_DOUBLE)}
	TYPE_NULL      = &T{InternalType: InternalType(client.MYSQL_TYPE_NULL)}
	TYPE_TIMESTAMP = &T{InternalType: InternalType(client.MYSQL_TYPE_TIMESTAMP)}
	TYPE_LONGLONG  = &T{InternalType: InternalType(client.MYSQL_TYPE_LONGLONG)}
	TYPE_INT24     = &T{InternalType: InternalType(client.MYSQL_TYPE_INT24)}
	TYPE_DATE      = &T{InternalType: InternalType(client.MYSQL_TYPE_DATE)}
	TYPE_DURATION  = &T{InternalType: InternalType(client.MYSQL_TYPE_TIME)}
	TYPE_TIME      = &T{InternalType: InternalType(client.MYSQL_TYPE_TIME)}
	TYPE_DATETIME  = &T{InternalType: InternalType(client.MYSQL_TYPE_DATETIME)}
	TYPE_YEAR      = &T{InternalType: InternalType(client.MYSQL_TYPE_YEAR)}
	TYPE_NEWDATE   = &T{InternalType: InternalType(client.MYSQL_TYPE_NEWDATE)}
	TYPE_VARCHAR   = &T{InternalType: InternalType(client.MYSQL_TYPE_VARCHAR)}
	TYPE_BIT       = &T{InternalType: InternalType(client.MYSQL_TYPE_BIT)}

	TYPE_BOOL        = &T{InternalType: InternalType(client.MYSQL_TYPE_BOOL)}
	TYPE_JSON        = &T{InternalType: InternalType(client.MYSQL_TYPE_JSON)}
	TYPE_ENUM        = &T{InternalType: InternalType(client.MYSQL_TYPE_ENUM)}
	TYPE_SET         = &T{InternalType: InternalType(client.MYSQL_TYPE_SET)}
	TYPE_TINY_BLOB   = &T{InternalType: InternalType(client.MYSQL_TYPE_TINY_BLOB)}
	TYPE_MEDIUM_BLOB = &T{InternalType: InternalType(client.MYSQL_TYPE_MEDIUM_BLOB)}
	TYPE_LONG_BLOB   = &T{InternalType: InternalType(client.MYSQL_TYPE_LONG_BLOB)}
	TYPE_BLOB        = &T{InternalType: InternalType(client.MYSQL_TYPE_BLOB)}
	TYPE_VARSTRING   = &T{InternalType: InternalType(client.MYSQL_TYPE_VAR_STRING)}
	TYPE_STRING      = &T{InternalType: InternalType(client.MYSQL_TYPE_STRING)}
	TYPE_GEOMETRY    = &T{InternalType: InternalType(client.MYSQL_TYPE_GEOMETRY)}
)
