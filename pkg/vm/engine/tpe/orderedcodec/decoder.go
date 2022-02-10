package orderedcodec

import "errors"

var (
	errorNoEnoughBytesForDecoding = errors.New("there is no enough bytes for decoding")
	errorIsNotNull = errors.New("it is not the null encoding")
)

//DecodeKey decodes
func (od *OrderedDecoder) DecodeKey(data []byte)([]byte, *DecodedItem,error){
	if data == nil || len(data) < 1 {
		return data,nil,errorNoEnoughBytesForDecoding
	}
	dataAfterNull,decodeItem,err := od.IsNull(data)
	if err == nil {
		return dataAfterNull,decodeItem,nil
	}
	return nil, nil, nil
}

// isNll decodes the NULL and returns the bytes after the null.
func (od *OrderedDecoder) IsNull(data []byte) ([]byte,*DecodedItem,error) {
	if data == nil || len(data) < 1 {
		return data,nil,errorNoEnoughBytesForDecoding
	}
	if data[0] != nullEncoding {
		return data,nil,errorIsNotNull
	}
	return data[1:], NewDecodeItem(nil,VALUE_TYPE_NULL,0,0,1), nil
}