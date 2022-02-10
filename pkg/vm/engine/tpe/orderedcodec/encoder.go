package orderedcodec

// EncodeKey encodes the value into the ordered bytes
func (oe *OrderedEncoder) EncodeKey(data []byte,value interface{})([]byte,*EncodedItem){
	if value == nil {
		return oe.EncodeNull(data)
	}
	return nil, nil
}

// EncodeNull encodes the NULL and appends the result to the buffer
func (oe *OrderedEncoder) EncodeNull(data []byte)([]byte,*EncodedItem){
	return append(data,nullEncoding), nil
}

// EncodeUint64 encodes the uint64 into ordered bytes and appends them to the buffer
func (oe *OrderedEncoder) EncodeUint64(data []byte,value uint64)([]byte,*EncodedItem) {
	return nil, nil
}