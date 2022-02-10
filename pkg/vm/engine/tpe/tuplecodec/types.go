package tuplecodec

type TupleKey []byte
type TupleValue []byte

// Range for [startKey, endKey)
type Range struct {
	startKey TupleKey
	endKey TupleKey
}

type TupleKeyEncoder struct {
	tenantPrefix *TupleKey
}

type EncodedItem struct {
	beginPos int
	size int
}

type TupleKeyDecoder struct {
	tenantPrefix *TupleKey
}