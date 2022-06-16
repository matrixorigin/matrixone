package types

var CompoundKeyType Type

func init() {
	CompoundKeyType = Type_VARCHAR.ToType()
	CompoundKeyType.Width = 100
}
