package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const DefaultReadBufferSize int = 512
const DefaultWriteBufferSize int = 512

type IOPackage interface {
	//read a packet with size from the connection
	ReadPacket(int)([]byte,error)

	//write a packet into the connection
	WritePacket([]byte)error

	//flush data into the connection
	Flush() error

	//close the connection
	Close()

	//get address Host:Port of the client
	Peer()(string,string)

	//the byte order
	//true - littleEndian; false - littleEndian
	IsLittleEndian() bool

	//write an uint8 into the buffer at the position
	//return the position + 1
	WriteUint8([]byte,int,uint8)int

	//write an uint16 with little endian into the buffer at the position
	//return the position + 2
	WriteUint16([]byte,int,uint16)int

	//write an uint32 with little endian into the buffer at the position
	//return the position + 4
	WriteUint32([]byte,int,uint32)int

	//write an uint64 with little endian into the buffer at the position
	//return the position + 8
	WriteUint64([]byte,int,uint64)int

	//append an uint8 to the buffer
	//return the buffer
	AppendUint8([]byte,uint8)[]byte

	//append an uint16 to the buffer
	//return the buffer
	AppendUint16([]byte,uint16)[]byte

	//append an uint32 to the buffer
	//return the buffer
	AppendUint32([]byte,uint32)[]byte

	//append an uint64 to the buffer
	//return the buffer
	AppendUint64([]byte,uint64)[]byte

	//read an uint8 with little endian from the buffer at the position
	//return uint8 value ; pos+1 ; true - decoded successfully or false - failed
	ReadUint8([]byte,int)(uint8,int,bool)

	//read an uint16 with little endian from the buffer at the position
	//return uint16 value ; pos+2 ; true - decoded successfully or false - failed
	ReadUint16([]byte,int)(uint16,int,bool)

	//read an uint32 with little endian from the buffer at the position
	//return uint32 value ; pos+4 ; true - decoded successfully or false - failed
	ReadUint32([]byte,int)(uint32,int,bool)

	//read an uint64 with little endian from the buffer at the position
	//return uint64 value ; pos+8 ; true - decoded successfully or false - failed
	ReadUint64([]byte,int)(uint64,int,bool)
}

//the IOPackageImpl implements the IOPackage for the basic interaction in the connection
type IOPackageImpl struct {
	tcp net.Conn
	bufferReader *bufio.Reader
	bufferWriter *bufio.Writer
	readBufferSize int
	writeBufferSize int
	//true - littleEndian; false - littleEndian
	endian bool
}

func NewIOPackage(in net.Conn, readBufferSize,writeBufferSize int,littleEndian bool) *IOPackageImpl{
	return &IOPackageImpl{
		tcp : in,
		readBufferSize: readBufferSize,
		writeBufferSize: writeBufferSize,
		bufferReader : bufio.NewReaderSize(in,readBufferSize),
		bufferWriter: bufio.NewWriterSize(in,writeBufferSize),
		endian: littleEndian,
	}
}

func (bio *IOPackageImpl) IsLittleEndian() bool {
	return bio.endian
}

func (bio *IOPackageImpl) ReadPacket(count int) ([]byte, error) {
	input := make([]byte,count)
	if rCount, err := io.ReadFull(bio.bufferReader,input); err !=nil {
		return input , err
	}else if rCount != count {
		return input, fmt.Errorf("wants %d bytes,but gets %d bytes. error: %v",count,rCount,err)
	}
	return input,nil
}

func (bio *IOPackageImpl) WritePacket(data []byte) error {
	if wCount,err :=bio.bufferWriter.Write(data); err != nil{
		fmt.Printf("written %d , all is %d \n",wCount,len(data))
		return err
	}else if wCount != len(data){
		return fmt.Errorf("write %d bytes,but succeeds %d bytes. error: %v",len(data), wCount,err)
	}
	return nil
}

func (bio *IOPackageImpl) Flush() error {
	return bio.bufferWriter.Flush()
}

func (bio *IOPackageImpl) Close() {
	bio.tcp.Close()
}

func (bio *IOPackageImpl) Peer() (string, string) {
	addr := bio.tcp.RemoteAddr().String()
	host,port,err := net.SplitHostPort(addr)
	if err != nil{
		fmt.Printf("get peer host:port failed. error:%v ",err)
		return "failed", "0"
	}
	return host,port
}

func (bio *IOPackageImpl) WriteUint8(data []byte,pos int,value uint8) int  {
	data[pos] = value
	return pos + 1
}

func (bio *IOPackageImpl) WriteUint16(data []byte, pos int, value uint16) int {
	if bio.endian{
		binary.LittleEndian.PutUint16(data[pos:],value)
	}else{
		binary.BigEndian.PutUint16(data[pos:],value)
	}
	return pos + 2
}

func (bio *IOPackageImpl) WriteUint32(data []byte, pos int, value uint32) int {
	if bio.endian{
		binary.LittleEndian.PutUint32(data[pos:],value)
	}else{
		binary.BigEndian.PutUint32(data[pos:],value)
	}
	return pos + 4
}

func (bio *IOPackageImpl) WriteUint64(data []byte, pos int, value uint64) int {
	if bio.endian{
		binary.LittleEndian.PutUint64(data[pos:],value)
	}else {
		binary.BigEndian.PutUint64(data[pos:],value)
	}
	return pos + 8
}

func (bio *IOPackageImpl) AppendUint8(data []byte,value uint8)[]byte{
	return append(data,value)
}

func (bio *IOPackageImpl) AppendUint16(data []byte,value uint16)[]byte{
	tmp := make([]byte,2)
	bio.WriteUint16(tmp,0,value)
	return append(data,tmp...)
}

func (bio *IOPackageImpl) AppendUint32(data []byte,value uint32)[]byte{
	tmp := make([]byte,4)
	bio.WriteUint32(tmp,0,value)
	return append(data,tmp...)
}

func (bio *IOPackageImpl) AppendUint64(data []byte,value uint64)[]byte{
	tmp := make([]byte,8)
	bio.WriteUint64(tmp,0,value)
	return append(data,tmp...)
}

func (bio *IOPackageImpl) ReadUint8(data []byte, pos int) (uint8, int, bool) {
	if pos >= len(data){
		return 0,0,false
	}
	return data[pos],pos+1,true
}

func (bio *IOPackageImpl) ReadUint16(data []byte,pos int)(uint16,int,bool)  {
	if pos + 1 >= len(data){
		return 0,0,false
	}
	if bio.endian {
		return binary.LittleEndian.Uint16(data[pos : pos+2]), pos + 2, true
	}else{
		return binary.BigEndian.Uint16(data[pos : pos+2]), pos + 2, true
	}
}

func (bio *IOPackageImpl) ReadUint32(data []byte, pos int) (uint32, int, bool) {
	if pos+3 >= len(data) {
		return 0, 0, false
	}
	if bio.endian{
		return binary.LittleEndian.Uint32(data[pos:pos+4]),pos+4,true
	}else{
		return binary.BigEndian.Uint32(data[pos:pos+4]),pos+4,true
	}
}

func (bio *IOPackageImpl) ReadUint64(data []byte, pos int) (uint64, int, bool) {
	if pos+7 >= len(data) {
		return 0, 0, false
	}
	if bio.endian{
		return binary.LittleEndian.Uint64(data[pos:pos+8]),pos+8,true
	}else{
		return binary.BigEndian.Uint64(data[pos:pos+8]),pos+8,true
	}
}

