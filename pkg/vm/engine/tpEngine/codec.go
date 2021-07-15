package tpEngine

import (
	"encoding/binary"
	"fmt"
	"strings"
)

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

/**
primary key type
*/
/**
string key encoding properties:

1. len(S) <= len(enc(S))
2.
if S = S1 + S2,then
enc(S1) must be prefix of enc(S)

3. Sa < Sb < Sc, enc(Sa) < enc(Sb) < enc(Sc) {lexicographic order}
4. S does not have tpEngineSlash, tpEngineConcat character.
5. enc(S) does not have the length of the S.
 */

func isValidStringKey(s string) bool{
	return !strings.ContainsAny(s,string([]byte{tpEngineSlash,tpEngineConcat}))
}

type stringKey string

func (sk stringKey) encode(data []byte) []byte {
	data = append(data,string2bytes(string(sk))...)
	return data
}

func (sk stringKey) String() string {
	return fmt.Sprintf("%s",string(sk))
}

type uint64Key uint64

func (uk uint64Key) encode(data []byte) []byte {
	//elementary method
	data = append(data,fmt.Sprintf("%d",uk)...)
	return data
}

func (uk uint64Key) String() string {
	return fmt.Sprintf("%v",uint64(uk))
}

/**
string a , b
if a < b, then encode(a) < encode(b) in bytes
*/
func encodeStringKey(data []byte,s string) []byte{
	return append(data, string2bytes(s)...)
}

/**
uint64 a , b
if a < b, then encode(a) < encode(b) in bytes
 */
func encodeUint64Key(data []byte,u uint64) []byte {
	return encodeUint64(data,u)
}

const sign uint64 = (1 << 63)

/**
int64 a , b
if a < b, then encode(a) < encode(b) in bytes
*/
func encodeInt64Key(data []byte,i int64) []byte {
	return encodeUint64(data,uint64(i) ^ sign)
}

//=======================================
// Separation line
//=======================================

/**
value type
*/
type stringValue string

func (sv stringValue) encodeValue(data []byte) []byte {
	data = append(data,TP_ENCODE_TYPE_STRING)
	return append(data,string2bytes(string(sv))...)
}

func (sv stringValue) String() string {
	return string(sv)
}

type uint64Value uint64

func (uv uint64Value) encodeValue(data []byte) []byte {
	v := uint64(uv)
	var tmp []byte = make([]byte,8)
	binary.BigEndian.PutUint64(tmp,v)
	//encode tage
	data = append(data,TP_ENCODE_TYPE_UINT64)
	return append(data,tmp...)
}

func (uv uint64Value) decodeValue(data []byte) {

}

func (uv uint64Value) String()string{
	return ""
}

/**
encode Value into bytes
*/
func encodeValue(data []byte,args ...interface{})[]byte{
	for _,arg := range args{
		switch a := arg.(type) {
		case uint64:
			data = append(data,TP_ENCODE_TYPE_UINT64)
			data = encodeUint64(data,a)
		case string:
			data = append(data,TP_ENCODE_TYPE_STRING)
			data = encodeString(data,a)
		default:
			panic(fmt.Errorf("unsupported value %v",a))
		}
	}
	return data
}

/**
decode bytes into Value
*/
func decodeValue(data []byte) ([]byte, interface{}, error) {
	if len(data) < 1 {
		return nil, nil, fmt.Errorf("invalid encoded bytes")
	}

	switch data[0] {
	case TP_ENCODE_TYPE_UINT64:
		nd,u,err := decodeUint64(data[1:])
		if err!= nil{
			return nil, nil, err
		}
		return nd, u, nil
	case TP_ENCODE_TYPE_STRING:
		nd,s,err := decodeString(data[1:])
		if err!= nil{
			return nil, nil, err
		}
		return nd, s, nil
	default:
		panic(fmt.Errorf("unsupported encode type %v",data[0]))
	}
}

/**
decode multi values
 */
func decodeCountOfValues(data []byte, count int) ([]byte, []interface{}, error) {
	var vals []interface{} = make([]interface{},count)
	for i:=0; i < count;i++ {
		nd,v,err := decodeValue(data)
		if err != nil {
			return nil, nil, err
		}
		data = nd
		vals[i] = v
	}
	return data, vals, nil
}

/**
decode all values
 */
func decodeValues(data []byte) ([]byte, []interface{}, error) {
	var vals []interface{}
	for len(data) != 0 {
		nd,v,err := decodeValue(data)
		if err != nil {
			return nil, nil, err
		}
		data = nd
		vals = append(vals,v)
	}
	return data, vals, nil
}

//with length
func encodeString(data []byte,s string)[]byte{
	//add length 8bytes
	data = encodeUint64(data,uint64(len(s)))
	return append(data,string2bytes(s)...)
}

func decodeString(data []byte)([]byte,string,error){
	nd,u,err := decodeUint64(data)
	if err != nil {
		return nil,"",err
	}
	return nd[u:],bytes2string(nd[:u]),nil
}

func encodeUint64(data []byte,u uint64)[]byte{
	var tmp []byte = make([]byte,8)
	binary.BigEndian.PutUint64(tmp,u)
	//encode tage
	return append(data,tmp...)
}

func decodeUint64(data []byte)([]byte,uint64,error){
	return data[8:],binary.BigEndian.Uint64(data),nil
}


func (tti *tpTupleImpl) encode(data []byte)[]byte{
	return encodeValue(data,tti.fields...)
}

//without column types ?
func (ttr *tpTupleImpl) decode(data []byte) ([]byte,error){
	nd,vals,err := decodeValues(data)
	if err != nil {
		return nd,err
	}
	ttr.fields = vals
	return nd,nil
}

func (ttr *tpTupleImpl) String() string {
	return fmt.Sprintf("%v",ttr.fields)
}

/**
encode/decode tableDatabasesRow
 */
func (tdr *tableDatabasesRow) encode(data []byte) []byte {
	return tdr.tpTupleImpl.encode(data)
}

func (tdr *tableDatabasesRow) decode(data []byte) ([]byte,error) {
	//field count 3
	nd,err := tdr.tpTupleImpl.decode(data)
	if err != nil {
		return nil,err
	}
	return nd,nil
}

func (tdr *tableDatabasesRow) String() string {
	return fmt.Sprintf("[%v %v %v] ",tdr.fields[0],tdr.fields[1],tdr.fields[2])
}

func (ttr *tableTablesRow) encode(data []byte) []byte  {
	//field count 3
	return encodeValue(data,ttr.tabname,ttr.tabid,ttr.tabschema)
}

func (ttr *tableTablesRow) decode(data []byte) ([]byte,error){
	//field count 3
	nd,vals,err := decodeCountOfValues(data,3)
	if err != nil {
		return data,nil
	}

	ttr.tabname = vals[0].(string)
	ttr.tabid = vals[1].(uint64)
	ttr.tabschema = vals[2].(string)
	return nd,nil
}

func (ttr *tableTablesRow) String() string {
	return fmt.Sprintf("[%v %v %v] ",ttr.tabname,ttr.tabid,ttr.tabschema)
}
