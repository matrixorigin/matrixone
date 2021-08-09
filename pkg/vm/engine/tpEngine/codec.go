package tpEngine

import (
	"bytes"
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

/**
encode keys
 */
func encodeKeys(data []byte,keys ...interface{})[]byte{
	for _,k := range keys {
		data = encodeKey(data,k)
	}
	return data
}

func encodeKeysWithSchema(data []byte,sch* tpSchema,keys ...interface{})[]byte{
	if len(keys) != sch.ColumnCount() {
		panic("miss fields or schemas")
	}
	for i,k := range keys {
		data = encodeKeyWithType(data,k,sch.ColumnType(i))
	}
	return data
}

/**
encode key
 */
func encodeKey(data []byte,key interface{})[]byte{
	switch k := key.(type) {
	case string:
		data = encodeStringKey(data,k)
	case uint64:
		data = encodeUint64Key(data,k)
	default:
		panic(fmt.Errorf("unsupported key %v",key))
	}
	return data
}

func encodeKeyWithType(data []byte,key interface{},keyType byte)[]byte{
	switch keyType {
	case TP_ENCODE_TYPE_STRING:
		data = encodeStringKey(data,key.(string))
	case TP_ENCODE_TYPE_UINT64:
		data = encodeUint64Key(data,key.(uint64))
	default:
		panic(fmt.Errorf("unsupported key %v",key))
	}
	return data
}

/**
decode keys with types
 */
func decodeKeys(data []byte, sch *tpSchema) ([]byte, []interface{}, error) {
	var vals []interface{}
	for i := 0 ; i < sch.ColumnCount() ; i++{
		nd,v,err := decodeKey(data,sch.ColumnType(i))
		if err != nil {
			return nil, nil, err
		}
		data = nd
		vals = append(vals,v)
	}
	return data,vals,nil
}

/**
decode key with type
 */
func decodeKey(data []byte,keyType byte)([]byte,interface{},error){
	switch keyType {
	case TP_ENCODE_TYPE_STRING:
		return decodeStringKey(data)
	case TP_ENCODE_TYPE_UINT64:
		return decodeUint64(data)
	default:
		panic(fmt.Errorf("unsupported key type %v",keyType))
	}
	return nil,nil,nil
}

/**
string a , b
if a < b, then encode(a) < encode(b) in bytes
*/
func encodeStringKey(data []byte,s string) []byte{
	return encodeStringKeyLimited(data,s,tpEngineMaxLengthOfStringFieldInSystemTable)
}

/**
decode string key
 */
func decodeStringKey(data []byte) ([]byte, string, error) {
	if len(data) < tpEngineMaxLengthOfStringFieldInSystemTable {
		return nil, "", fmt.Errorf("missing bytes")
	}

	padIdx := bytes.IndexByte(data,0)
	var s string
	if padIdx == -1 {
		s = bytes2string(data[:tpEngineMaxLengthOfStringFieldInSystemTable])
	} else{
		s = bytes2string(data[:padIdx])
	}

	return data[tpEngineMaxLengthOfStringFieldInSystemTable:],s,nil
}

/**
string a , b
if a < b, then encode(a) < encode(b) in bytes

if len(s) < length:
	pad with 0;
	encode s;
else if len(s) == length:
	encode s;
else:
	encode the prefix of s with length's bytes
*/
func encodeStringKeyLimited(data []byte, s string, length int) []byte {
	l := len(s)
	if l < length {
		data = append(data,string2bytes(s)...)
		pads := make([]byte,length - l)
		for i:= 0; i < len(pads);i++{
			pads[i] = 0
		}
		return append(data,pads...)
	}else if l == length {
		return append(data, string2bytes(s)...)
	}else{
		panic("the string key is too much long")
		return append(data, string2bytes(s[:length])...)
	}
}

/*
decode string key with limited length
 */
func decodeStringKeyLimited(data []byte, length int) ([]byte, string, error) {
	if len(data) < length {
		return nil, "", fmt.Errorf("missing bytes")
	}
	s := bytes2string(data[:length])
	return data[length:],s,nil
}

/**
uint64 a , b
if a < b, then encode(a) < encode(b) in bytes
 */
func encodeUint64Key(data []byte,u uint64) []byte {
	return encodeUint64(data,u)
}

const sign64 uint64 = (1 << 63)

/**
int64 a , b
if a < b, then encode(a) < encode(b) in bytes
*/
func encodeInt64Key(data []byte,i int64) []byte {
	return encodeUint64(data,uint64(i) ^sign64)
}

//=======================================
// Separation line
//=======================================

/**
encode Value into bytes
*/
func encodeValues(data []byte,args ...interface{})[]byte{
	for _,arg := range args{
		switch a := arg.(type) {
		case uint64:
			data = append(data,TP_ENCODE_TYPE_UINT64)
			data = encodeUint64(data,a)
		case string:
			data = append(data,TP_ENCODE_TYPE_STRING)
			data = encodeString(data,a)
		case []byte:
			data = append(data,TP_ENCODE_TYPE_BYTES)
			data = encodeBytes(data,a)
		default:
			panic(fmt.Errorf("unsupported value %v",arg))
		}
	}
	return data
}

/**
encode Value into bytes with schema
*/
func encodeValuesWithSchema(sch *tpSchema,data []byte,args ...interface{})[]byte{
	for i := 0; i < sch.ColumnCount(); i += 1{
		if !sch.IsUsedInEncoding(i) {
			continue
		}
		t := sch.ColumnType(i)
		switch t {
		case TP_ENCODE_TYPE_UINT64:
			data = append(data,TP_ENCODE_TYPE_UINT64)
			data = encodeUint64(data,args[i].(uint64))
		case TP_ENCODE_TYPE_STRING:
			data = append(data,TP_ENCODE_TYPE_STRING)
			data = encodeString(data,args[i].(string))
		case TP_ENCODE_TYPE_BYTES:
			data = append(data,TP_ENCODE_TYPE_BYTES)
			data = encodeBytes(data,args[i].([]byte))
		default:
			panic(fmt.Errorf("unsupported column type %v",t))
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
	case TP_ENCODE_TYPE_BYTES:
		nd,b,err := decodeBytes(data[1:])
		if err!= nil{
			return nil, nil, err
		}
		return nd, b, nil
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
func encodeBytes(data []byte,bts []byte) []byte {
	//add length 8bytes
	data = encodeUint64(data,uint64(len(bts)))
	return append(data,bts...)
}

func decodeBytes(data []byte)([]byte,[]byte,error) {
	nd,u,err := decodeUint64(data)
	if err != nil {
		return nil,nil,err
	}
	return nd[u:],nd[:u],nil
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

/**
tuple encode/decode
 */

func (tti *tpTupleImpl) encode(data []byte)[]byte{
	if tti.schema == nil {
		return encodeValues(data,tti.fields...)
	}else{
		return encodeValuesWithSchema(tti.schema,data,tti.fields...)
	}
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