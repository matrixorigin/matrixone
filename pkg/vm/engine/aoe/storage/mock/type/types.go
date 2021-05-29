package mocktype

import ()

type ColType uint16

const (
	INVALID ColType = iota
	BOOLEAN
	SMALLINT
	INTEGER
	BIGINT

	STRING
)

func (t ColType) Size() uint64 {
	switch t {
	case BOOLEAN:
		return uint64(1)
	case SMALLINT:
		return uint64(2)
	case INTEGER:
		return uint64(4)
	case BIGINT:
		return uint64(8)
	case STRING:
		return uint64(1)
	}
	panic("Unsupported")
}

func (t ColType) IsStr() bool {
	switch t {
	case STRING:
		return true
	}
	return false
}
