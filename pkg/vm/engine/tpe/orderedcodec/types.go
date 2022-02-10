package orderedcodec

type OrderedEncoder struct {

}

type EncodedItem struct {
	//count of encoded bytes
	size int
}

type OrderedDecoder struct {

}

type ValueType int
const (
	VALUE_TYPE_UNKOWN ValueType = 0x0
	VALUE_TYPE_NULL ValueType = 0x1

)

type DecodedItem struct {
	value    interface{}
	valueType ValueType // int,uint,uint64,...,float
	sectionType int //belongs to which section
	offsetInUndecodedKey int    //the position in undecoded bytes
	bytesCountInUndecodedKey int //the count of bytes in undecoded bytes
}

func NewDecodeItem(v interface{}, vt ValueType, st int,oiu int,bciu int) *DecodedItem {
	return &DecodedItem{
		value:                    v,
		valueType:                vt,
		sectionType:              st,
		offsetInUndecodedKey:     oiu,
		bytesCountInUndecodedKey: bciu,
	}
}