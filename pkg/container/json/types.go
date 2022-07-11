package json

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/errno"
)

type TpCode byte

const (
	TpCodeObject  = 0x01
	TpCodeArray   = 0x02
	TpCodeLiteral = 0x03
	TpCodeInt64   = 0x04
	TpCodeUint64  = 0x05
	TpCodeFloat64 = 0x06
	TpCodeString  = 0x07
)

var (
	ErrEmptyJsonText   = errors.New(errno.EmptyJsonText)
	ErrInvalidJsonText = errors.New(errno.InvalidJsonText)
)

const (
	LiteralNull  byte = 0x00
	LiteralTrue  byte = 0x01
	LiteralFalse byte = 0x02
)
