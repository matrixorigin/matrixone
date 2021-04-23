package server

import (
	"fmt"
	"net"
	"testing"
)

func TestReadIntLenEnc(t *testing.T) {
	var intEnc MysqlClientProtocol
	var data=make([]byte,24)
	var cases=[][]uint64{
		{0,123,250},
		{251, 10000,1 << 16 - 1},
		{1 << 16,1 << 16 + 10000,1 << 24 - 1},
		{1 << 24,1 << 24 + 10000,1 << 64 - 1},
	}
	var caseLens =[]int{1,3,4,9}
	for j:=0; j < len(cases);j++{
		for i:=0 ; i < len(cases[j]);i++{
			value := cases[j][i]
			p1 := intEnc.writeIntLenEnc(data,0,value)
			val,p2,ok := intEnc.readIntLenEnc(data,0)
			if !ok || p1 != caseLens[j] || p1 != p2 || val != value{
				t.Errorf("IntLenEnc %d failed.",value)
				break
			}
			val,p2,ok = intEnc.readIntLenEnc(data[0:caseLens[j]-1],0)
			if ok{
				t.Errorf("read IntLenEnc failed.")
				break
			}
		}
	}
}

func TestReadCountOfBytes(t *testing.T) {
	var client MysqlClientProtocol
	var data=make([]byte,24)
	var length int = 10
	for i:= 0; i < length;i++{
		data[i] = byte(length - i)
	}

	r,pos,ok := client.readCountOfBytes(data,0, length)
	if !ok || pos != length {
		t.Error("read bytes failed.")
		return
	}

	for i:=0;i< length;i++{
		if r[i] != data[i]{
			t.Error("read != write")
			break
		}
	}

	r,pos,ok = client.readCountOfBytes(data,0,100)
	if ok{
		t.Error("read bytes failed.")
		return
	}

	r,pos,ok = client.readCountOfBytes(data,0,0)
	if !ok || pos != 0{
		t.Error("read bytes failed.")
		return
	}
}

func TestReadStringFix(t *testing.T) {
	var client MysqlClientProtocol
	var data=make([]byte,24)
	var length int = 10
	var s string="haha, test read string fix function"
	pos := client.writeStringFix(data,0,s,length)
	if pos != length{
		t.Error("write string fix failed.")
		return
	}
	var x string
	var ok bool

	x,pos,ok=client.readStringFix(data,0,length)
	if !ok || pos != length || x != s[0:length]{
		t.Error("read string fix failed.")
		return
	}
	var sLen =[]int{
		length+10,
		length+20,
		length+30,
	}
	for i:=0; i < len(sLen);i++{
		x,pos,ok = client.readStringFix(data,0,sLen[i])
		if ok && pos == sLen[i] && x == s[0:sLen[i]] {
			t.Error("read string fix failed.")
			return
		}
	}

	//empty string
	pos = client.writeStringFix(data,0,s,0)
	if pos != 0{
		t.Error("write string fix failed.")
		return
	}

	x,pos,ok=client.readStringFix(data,0,0)
	if !ok || pos != 0 || x != ""{
		t.Error("read string fix failed.")
		return
	}
}

func TestReadStringNUL(t *testing.T) {
	var client MysqlClientProtocol
	var data=make([]byte,24)
	var length int = 10
	var s string="haha, test read string fix function"
	pos := client.writeStringNUL(data,0,s[0:length])
	if pos != length+1{
		t.Error("write string NUL failed.")
		return
	}
	var x string
	var ok bool

	x,pos,ok=client.readStringNUL(data,0)
	if !ok || pos != length+1 || x != s[0:length]{
		t.Error("read string NUL failed.")
		return
	}
	var sLen =[]int{
		length+10,
		length+20,
		length+30,
	}
	for i:=0; i < len(sLen);i++{
		x,pos,ok = client.readStringNUL(data,0)
		if ok && pos == sLen[i]+1 && x == s[0:sLen[i]] {
			t.Error("read string NUL failed.")
			return
		}
	}
}

func TestReadStringLenEnc(t *testing.T) {
	var client MysqlClientProtocol
	var data=make([]byte,24)
	var length int = 10
	var s string="haha, test read string fix function"
	pos := client.writeStringLenEnc(data,0,s[0:length])
	if pos != length+1{
		t.Error("write string lenenc failed.")
		return
	}
	var x string
	var ok bool

	x,pos,ok=client.readStringLenEnc(data,0)
	if !ok || pos != length+1 || x != s[0:length]{
		t.Error("read string lenenc failed.")
		return
	}

	//empty string
	pos = client.writeStringLenEnc(data,0,s[0:0])
	if pos != 1{
		t.Error("write string lenenc failed.")
		return
	}

	x,pos,ok=client.readStringLenEnc(data,0)
	if !ok || pos != 1 || x != s[0:0]{
		t.Error("read string lenenc failed.")
		return
	}
}

func handshakeHandler(in net.Conn){
	var err error
	io := NewIOPackage(in,512,512,true)
	defer io.Close()
	fmt.Println("Server handling")
	mysql := NewMysqlClientProtocol(io,0)
	if err = mysql.Handshake(); err!=nil{
		msg := fmt.Sprintf("handshake failed. error:%v",err)
		mysql.sendErrPacket(ErrUnknown,DefaultMySQLState,msg)
		return
	}

	var req *Request
	var resp *Response
	for{
		//The sequence-id is incremented with each packet and may wrap around.
		//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
		mysql.setSequenceID(0)

		if req,err =mysql.ReadRequest(); err!=nil{
			fmt.Printf("read request failed. error:%v",err)
			break
		}
		switch uint8(req.GetCmd()){
		case COM_QUIT:
			resp = &Response{
			category: okResponse,
			status: 0,
			data:nil,
			}
			if err = mysql.SendResponse(resp); err != nil{
				fmt.Printf("send response failed. error:%v",err)
				break
			}
		case COM_QUERY:
			var query =string(req.GetData().([]byte))
			fmt.Printf("query: %s \n",query)
			resp = &Response{
				category: okResponse,
				status: 0,
				data:nil,
			}
			if err = mysql.SendResponse(resp); err != nil{
				fmt.Printf("send response failed. error:%v",err)
				break
			}
		default:
			fmt.Printf("unsupported command. 0x%x \n",req.cmd)
		}
		if uint8(req.cmd) == COM_QUIT{
			break
		}
	}

}

func TestMysqlClientProtocol_Handshake(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connection method: mysql -h 127.0.0.1 -P 6001 -uroot -p
	echoServer(handshakeHandler)
}