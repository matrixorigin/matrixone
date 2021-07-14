package tpEngine

import "fmt"

/**
encode the data into the bytes in ascending order:
if a < b
then encode(a) < encode(b) in bytes.
 */

const (
	TP_ENCODE_TYPE_NULL byte = iota
	TP_ENCODE_TYPE_BYTES
	TP_ENCODE_TYPE_STRING
	TP_ENCODE_TYPE_UINT64
)

const (
	ChunkSize = 8
	ChunkPad = byte(0)
	ChunkEnd = byte(255)

)

var PadBytes = make([]byte,ChunkSize)

//encode bytes into comparable and ascending bytes
func encodeComparableBytes(data []byte,val []byte) []byte  {
	/**
	rule:
	spilt bytes into many chunks with 8 bytes.
	the last chunk will be padded with 0s at the ChunkEnd.
	then each chunk is ended with an value: 255 - padding count.
	 */
	vlen := len(val)
	for i := 0; i <= vlen; i+= ChunkSize {
		r := vlen - i
		var pads byte = 0
		if r >= ChunkSize {
			data = append(data,val[i:i+ChunkSize]...)
		}else{
			pads = byte(ChunkSize - r)
			data = append(data,val[i:]...)
			data = append(data,PadBytes[:pads]...)
		}

		data = append(data,ChunkEnd - pads)
	}

	return data
}

//make comparable bytes
func makeComparableBytes(data []byte,val []byte) []byte {
	data = append(data,TP_ENCODE_TYPE_BYTES)
	return encodeComparableBytes(data,val)
}

//encode string into comparable and ascending bytes
func encodeComparableString(data []byte,val string) []byte {
	return encodeComparableBytes(data,[]byte(val))
}

//make comparable string
func makeComparableString(data []byte,val string) []byte {
	data = append(data,TP_ENCODE_TYPE_STRING)
	return encodeComparableString(data,val)
}

//decode comparable and ascending bytes into original bytes
func decodeComparableBytes(data []byte,enc []byte) ([]byte,error) {
	for i := 0; i < len(enc); i += ChunkSize + 1 {
		if len(enc) -  i < ChunkSize + 1 {
			return nil,fmt.Errorf("invalide encoded string")
		}

		chunkEnd := enc[:i + ChunkSize+1]

		chunk := chunkEnd[i:i+ChunkSize]
		end := chunkEnd[i+ChunkSize]
		pads := ChunkEnd - end

		if pads > ChunkSize {
			return nil,fmt.Errorf("invalid pad count %d",pads)
		}

		data = append(data,chunk[:ChunkSize - pads]...)
	}
	return data,nil
}