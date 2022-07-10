package json

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
