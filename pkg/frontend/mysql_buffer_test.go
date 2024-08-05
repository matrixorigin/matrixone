// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/smartystreets/goconvey/convey"
	"math/rand"
	"net"
	"reflect"
	"testing"
	"time"
)

func hasData(conn net.Conn) (bool, error) {
	timeout := 1 * time.Second
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}

func generateRandomBytes(n int) []byte {
	data := make([]byte, n)
	return data
}

func TestMySQLBufferRead(t *testing.T) {
	var err error
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 24 * time.Hour
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	cm, err := NewIOSession(client, pu)
	if err != nil {
		panic(err)
	}
	convey.Convey("read small packet < 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 5 // 16MB
		seqID := byte(0)
		go func() {
			for i := 0; i < repeat; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header, uint32(packetSize))
				header[3] = seqID

				payload := make([]byte, packetSize)
				for j := range payload {
					payload[j] = byte(rand.Intn(100) + 1)
				}
				exceptPayload = append(exceptPayload, payload)
				_, err := server.Write(header)
				if err != nil {
					panic(err)
				}

				_, err = server.Write(payload)
				if err != nil {
					panic(err)
				}
			}
		}()
		var data []byte
		for i := 0; i < repeat; i++ {
			data, err = cm.Read()
			if err != nil {
				t.Fatalf("Failed to read payload: %v", err)
			}
			actualPayload = append(actualPayload, data)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})

}

func TestMySQLBufferReadLoadLocal(t *testing.T) {
	var err error
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(NewSessionAllocator(pu))
	convey.Convey("test read load local packet", t, func() {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, _ := NewIOSession(client, pu)
		cReader, _ := NewIOSession(server, pu)

		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		go func() {
			var err error
			fieldSizes := []int{1000, 2000, 1000}
			for i := 0; i < 3; i++ {
				exceptRow := make([]byte, 0)
				err = cWriter.BeginPacket()
				if err != nil {
					panic(err)
				}
				field := generateRandomBytes(fieldSizes[i])
				exceptRow = append(exceptRow, field...)
				err = cWriter.Append(field...)
				if err != nil {
					panic(err)
				}
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				if err != nil {
					panic(err)
				}
			}
			err = cWriter.Flush()
			if err != nil {
				panic(err)
			}
		}()

		var err error
		var data []byte
		for i := 0; i < 3; i++ {
			data, err = cReader.ReadLoadLocalPacket()
			payload := make([]byte, len(data))
			copy(payload, data)
			if err != nil {
				panic(err)
			}
			actualPayload = append(actualPayload, payload)
		}
		remain, err := hasData(server)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		convey.So(remain, convey.ShouldBeFalse)

	})

}

func TestMySQLBufferWriteRows(t *testing.T) {
	var err error
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(NewSessionAllocator(pu))
	convey.Convey("test write packet", t, func() {
		rows := 20
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, _ := NewIOSession(client, pu)
		cReader, _ := NewIOSession(server, pu)

		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		columns := rand.Intn(20) + 1
		fieldSize := rand.Intn(20) + 1
		go func() {
			var err error
			for i := 0; i < rows; i++ {
				exceptRow := make([]byte, 0)
				err = cWriter.BeginPacket()
				if err != nil {
					panic(err)
				}
				for j := 0; j < columns; j++ {
					field := generateRandomBytes(fieldSize)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					if err != nil {
						panic(err)
					}
				}
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				if err != nil {
					panic(err)
				}
			}
			err = cWriter.Flush()
			if err != nil {
				panic(err)
			}
		}()

		var data []byte
		var err error
		for i := 0; i < rows; i++ {
			data, err = cReader.Read()
			if err != nil {
				panic(err)
			}
			actualPayload = append(actualPayload, data)
		}
		remain, err := hasData(server)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		convey.So(remain, convey.ShouldBeFalse)

	})

}

func TestMySQLBufferMaxAllowedPacket(t *testing.T) {
	var err error
	var remain bool
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(NewSessionAllocator(pu))
	convey.Convey("test read max allowed packet", t, func() {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, _ := NewIOSession(client, pu)
		cReader, _ := NewIOSession(server, pu)
		ses := &Session{}
		ses.respr = &MysqlResp{
			mysqlRrWr: &MysqlProtocolImpl{io: NewIOPackage(true), tcpConn: cReader},
		}
		cReader.ses = ses
		exceptPayload := make([][]byte, 0)
		go func() {
			for {
				_, err := cWriter.Read()
				if err != nil {
					return
				}
			}
		}()
		go func() {
			var err error

			err = cWriter.BeginPacket()
			if err != nil {
				panic(err)
			}
			exceptRow := generateRandomBytes(int(MaxPayloadSize) / 2)
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			if err != nil {
				panic(err)
			}
			err = cWriter.FinishedPacket()
			if err != nil {
				panic(err)
			}

			err = cWriter.BeginPacket()
			if err != nil {
				panic(err)
			}
			exceptRow = generateRandomBytes(int(MaxPayloadSize))
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			if err != nil {
				panic(err)
			}
			err = cWriter.FinishedPacket()
			if err != nil {
				panic(err)
			}

			err = cWriter.BeginPacket()
			if err != nil {
				panic(err)
			}
			exceptRow = generateRandomBytes(int(MaxPayloadSize) * 2)
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			if err != nil {
				panic(err)
			}
			err = cWriter.FinishedPacket()
			if err != nil {
				panic(err)
			}

			err = cWriter.Flush()
			if err != nil {
				panic(err)
			}
		}()

		var data []byte
		data, err = cReader.Read()
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(data, exceptPayload[0]), convey.ShouldBeTrue)
		data, err = cReader.Read()
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(data, exceptPayload[1]), convey.ShouldBeTrue)
		data, err = cReader.Read()
		convey.So(err, convey.ShouldNotBeNil)
		for remain, _ = hasData(server); remain; remain, _ = hasData(server) {
			_, _ = cReader.conn.Read(make([]byte, int(MaxPayloadSize)))
		}
	})
}
