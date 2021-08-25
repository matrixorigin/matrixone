package frontend

import (
	"fmt"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/simple"
	"matrixone/pkg/config"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicIOPackage_WriteUint8(t *testing.T) {
	var buffer = make([]byte, 256)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 256; i++ {
		prePos = pos
		pos = IO.WriteUint8(buffer, pos, uint8(i))
		if pos != prePos+1 {
			t.Errorf("WriteUint8 value %d failed.", i)
			break
		}
		if buffer[i] != uint8(i) {
			t.Errorf("WriteUint8 value %d failed.", i)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint16(t *testing.T) {
	var buffer = make([]byte, 65536*2)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint16(i)
		prePos = pos
		pos = IO.WriteUint16(buffer, pos, value)
		if pos != prePos+2 {
			t.Errorf("WriteUint16 value %d failed.", value)
			break
		}

		var b1, b2 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
		} else {
			b1 = uint8((value >> 8) & 0xff)
			b2 = uint8(value & 0xff)
		}

		p := 2 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2) {
			t.Errorf("WriteUint16 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint32(t *testing.T) {
	var buffer = make([]byte, 65536*4)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint32(0x01010101 + i)
		prePos = pos
		pos = IO.WriteUint32(buffer, pos, value)
		if pos != prePos+4 {
			t.Errorf("WriteUint32 value %d failed.", value)
			break
		}
		var b1, b2, b3, b4 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
			b3 = uint8((value >> 16) & 0xff)
			b4 = uint8((value >> 24) & 0xff)
		} else {
			b4 = uint8(value & 0xff)
			b3 = uint8((value >> 8) & 0xff)
			b2 = uint8((value >> 16) & 0xff)
			b1 = uint8((value >> 24) & 0xff)
		}

		p := 4 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2 && buffer[p+2] == b3 && buffer[p+3] == b4) {
			t.Errorf("WriteUint32 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint64(t *testing.T) {
	var buffer = make([]byte, 65536*8)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint64(0x0101010101010101 + i)
		prePos = pos
		pos = IO.WriteUint64(buffer, pos, value)
		if pos != prePos+8 {
			t.Errorf("WriteUint64 value %d failed.", value)
			break
		}
		var b1, b2, b3, b4, b5, b6, b7, b8 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
			b3 = uint8((value >> 16) & 0xff)
			b4 = uint8((value >> 24) & 0xff)
			b5 = uint8((value >> 32) & 0xff)
			b6 = uint8((value >> 40) & 0xff)
			b7 = uint8((value >> 48) & 0xff)
			b8 = uint8((value >> 56) & 0xff)
		} else {
			b8 = uint8(value & 0xff)
			b7 = uint8((value >> 8) & 0xff)
			b6 = uint8((value >> 16) & 0xff)
			b5 = uint8((value >> 24) & 0xff)
			b4 = uint8((value >> 32) & 0xff)
			b3 = uint8((value >> 40) & 0xff)
			b2 = uint8((value >> 48) & 0xff)
			b1 = uint8((value >> 56) & 0xff)
		}

		p := 8 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2 && buffer[p+2] == b3 && buffer[p+3] == b4 &&
			buffer[p+4] == b5 && buffer[p+5] == b6 && buffer[p+6] == b7 && buffer[p+7] == b8) {
			t.Errorf("WriteUint64 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint8(t *testing.T) {
	var buffer = make([]byte, 256)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 256; i++ {
		prePos = pos
		IO.WriteUint8(buffer, pos, uint8(i))
		rValue, pos, ok := IO.ReadUint8(buffer, pos)
		if !ok || rValue != uint8(i) || pos != prePos+1 {
			t.Errorf("ReadUint8 value %d failed.", i)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint16(t *testing.T) {
	var buffer = make([]byte, 65536*2)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint16(i)
		prePos = pos
		IO.WriteUint16(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint16(buffer, pos)
		if !ok || pos != prePos+2 || rValue != value {
			t.Errorf("ReadUint16 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint32(t *testing.T) {
	var buffer = make([]byte, 65536*4)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint32(0x01010101 + i)
		prePos = pos
		IO.WriteUint32(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint32(buffer, pos)
		if !ok || pos != prePos+4 || rValue != value {
			t.Errorf("ReadUint32 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint64(t *testing.T) {
	var buffer = make([]byte, 65536*8)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint64(0x0101010101010101 + i)
		prePos = pos
		IO.WriteUint64(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint64(buffer, pos)
		if !ok || pos != prePos+8 || rValue != value {
			t.Errorf("ReadUint64 value %d failed.", value)
			break
		}
	}
}

var svrRun int32

func isClosed()bool {
	return atomic.LoadInt32(&svrRun) != 0
}

func setServer(val int32) {
	atomic.StoreInt32(&svrRun, val)
}

func echoHandler(session goetty.IOSession) {
	fmt.Println("Server handling")
	for {
		data, err := session.Read()
		if err != nil {
			fmt.Println("read packet failed")
			break
		}

		value, ok := data.(uint16)
		if !ok {
			fmt.Println("convert to uint16 failed.")
			break
		}

		fmt.Printf("server read %d\n", value)

		err = session.WriteAndFlush(value)
		if err != nil {
			fmt.Printf("write uint16 failed\n")
			return
		}

		fmt.Printf("server send %d\n", value)
		if value == 0 { //0 -- quit
			break
		}
	}
	err := session.Close()
	if err != nil {
		return 
	}
}

func echoServer(handler func (conn goetty.IOSession)) {
	server := NewMOServer("localhost:6001", nil, nil)
	err := server.Start()
	if err != nil {
		return
	}
	setServer(0)
	err = config.GlobalSystemVariables.LoadInitialValues()
	if err != nil {
		return 
	}
	err = config.LoadvarsConfigFromFile("../config/system_vars_config.toml", &config.GlobalSystemVariables)
	if err != nil {
		return 
	}

	fmt.Println("Server started")
	for !isClosed() {
		session, _ := server.app.GetSession(0)
		handler(session)
	}
	fmt.Println("Server exited")
}

func echoClient() {
	addrPort := "localhost:6001"
	encoder, decoder := simple.NewStringCodec()
	io := goetty.NewIOSession(goetty.WithCodec(encoder, decoder))
	_, err := io.Connect(addrPort, time.Second*3)
	if err != nil {
		fmt.Println("connect server failed.", err.Error())
		return
	}

	for i := 10; i >= 0; i-- {
		err := io.WriteAndFlush(uint16(i))
		if err != nil {
			fmt.Println("client write packet failed.", err.Error())
			break
		}
		fmt.Printf("client write %d \n", i)
		data, err := io.Read()
		if err != nil {
			fmt.Println("client read packet failed.", err.Error())
			break
		}
		value, ok := data.(uint16)
		if !ok {
			fmt.Println("convert to uint16 failed.")
			break
		}
		fmt.Printf("client read %d \n", value)
		if value != uint16(i) {
			fmt.Printf("echo failed. send %d but reponse %d\n", i, value)
			break
		}
	}
	err = io.Close()
	if err != nil {
		return
	}
}

func TestIOPackageImpl_ReadPacket(t *testing.T) {
	go echoServer(echoHandler)
	time.Sleep(1 * time.Second)
	echoClient()
}