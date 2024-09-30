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
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
)

func ReadPacketForTest(c *Conn) ([]byte, error) {
	// Requests > 16MB
	payloads := make([][]byte, 0)
	totalLength := 0
	var finalPayload []byte
	var payload []byte
	var err error
	defer func() {
		for i := range payloads {
			c.allocator.Free(payloads[i])
		}
	}()

	for {
		var packetLength int
		err = c.ReadNBytesIntoBuf(c.header[:], HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		packetLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
		sequenceId := c.header[3]
		c.sequenceId = sequenceId + 1

		if packetLength == 0 {
			break
		}
		totalLength += packetLength
		err = c.CheckAllowedPacketSize(totalLength)
		if err != nil {
			return nil, err
		}

		if totalLength != int(MaxPayloadSize) && len(payloads) == 0 {
			signalPayload := make([]byte, totalLength)
			err = c.ReadNBytesIntoBuf(signalPayload, totalLength)
			if err != nil {
				return nil, err
			}
			return signalPayload, nil
		}

		payload, err = c.ReadOnePayload(packetLength)
		if err != nil {
			return nil, err
		}

		payloads = append(payloads, payload)

		if uint32(packetLength) != MaxPayloadSize {
			break
		}
	}

	if totalLength > 0 {
		finalPayload = make([]byte, totalLength)
	}

	copyIndex := 0
	for _, eachPayload := range payloads {
		copy(finalPayload[copyIndex:], eachPayload)
		copyIndex += len(eachPayload)
	}
	return finalPayload, nil
}

func stumblingToWrite(t *testing.T, conn net.Conn, packet []byte) {
	numParts := rand.Intn(3) + 2
	splitPacket := make([][]byte, numParts)
	currentIndex := 0
	for i := 0; i < numParts-1; i++ {
		remainingElements := len(packet) - currentIndex
		maxSize := remainingElements - (numParts - i - 1)
		partSize := rand.Intn(maxSize) + 1
		splitPacket[i] = packet[currentIndex : currentIndex+partSize]
		currentIndex += partSize
	}
	splitPacket[numParts-1] = packet[currentIndex:]
	for i := range splitPacket {
		_, err := conn.Write(splitPacket[i])
		assert.Nil(t, err)
	}
}

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
	randomIndex := rand.Intn(len(data))
	data[randomIndex] = 1
	return data
}

func TestMySQLProtocolRead(t *testing.T) {
	var err error
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 24 * time.Hour
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(newLeakCheckAllocator())
	cm, err := NewIOSession(client, pu)
	assert.Nil(t, err)
	cm.allowedPacketSize = int(MaxPayloadSize) * 16
	convey.Convey("read small packet < 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 5 // 5KB
		go func() {
			for i := 0; i < repeat; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header, uint32(packetSize))
				header[3] = byte(i)

				payload := generateRandomBytes(packetSize)
				exceptPayload = append(exceptPayload, payload)
				_, err := server.Write(append(header, payload...))
				assert.Nil(t, err)
			}
		}()
		var data []byte
		for i := 0; i < repeat; i++ {
			data, err = cm.Read()
			assert.Nil(t, err)
			actualPayload = append(actualPayload, data)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})

	convey.Convey("read small packet > 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 1024 * 5 // 5MB
		go func() {
			for i := 0; i < repeat; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header, uint32(packetSize))
				header[3] = byte(i)

				payload := generateRandomBytes(packetSize)
				exceptPayload = append(exceptPayload, payload)
				_, err := server.Write(append(header, payload...))
				assert.Nil(t, err)
			}
		}()
		var data []byte
		for i := 0; i < repeat; i++ {
			data, err = cm.Read()
			assert.Nil(t, err)
			actualPayload = append(actualPayload, data)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)

	})

	convey.Convey("read big packet", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				if i == 2 {
					packetSize -= 1
				}
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = byte(i)

				payload := generateRandomBytes(int(packetSize))
				exceptPayload = append(exceptPayload, payload...)
				_, err := server.Write(append(header, payload...))
				assert.Nil(t, err)
			}
		}()

		actualPayload, err := cm.Read()
		assert.Nil(t, err)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)

	})

	convey.Convey("read big packet, the last package size is equal to 16MB", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = byte(i)
				payload := generateRandomBytes(int(packetSize))
				exceptPayload = append(exceptPayload, payload...)
				_, err := server.Write(append(header, payload...))
				assert.Nil(t, err)
			}
			header := make([]byte, 4)
			binary.LittleEndian.PutUint32(header[:4], 0)
			header[3] = byte(totalPackets)
			_, err := server.Write(header)
			assert.Nil(t, err)
		}()

		actualPayload, err := cm.Read()
		assert.Nil(t, err)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})
}

func TestMySQLProtocolReadInBadNetwork(t *testing.T) {
	var err error
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 24 * time.Hour
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(newLeakCheckAllocator())
	cm, err := NewIOSession(client, pu)
	assert.Nil(t, err)
	cm.allowedPacketSize = int(MaxPayloadSize) * 16
	convey.Convey("Bad Network: read small packet < 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 5 // 5KB
		go func() {
			for i := 0; i < repeat; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header, uint32(packetSize))
				header[3] = byte(i)

				payload := generateRandomBytes(packetSize)
				exceptPayload = append(exceptPayload, payload)
				stumblingToWrite(t, server, append(header, payload...))
			}
		}()
		var data []byte
		for i := 0; i < repeat; i++ {
			data, err = cm.Read()
			assert.Nil(t, err)
			actualPayload = append(actualPayload, data)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})

	convey.Convey("Bad Network: read small packet > 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 1024 * 5 // 5MB
		go func() {
			for i := 0; i < repeat; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header, uint32(packetSize))
				header[3] = byte(i)

				payload := generateRandomBytes(packetSize)
				exceptPayload = append(exceptPayload, payload)
				stumblingToWrite(t, server, append(header, payload...))
			}
		}()
		var data []byte
		for i := 0; i < repeat; i++ {
			data, err = cm.Read()
			assert.Nil(t, err)
			actualPayload = append(actualPayload, data)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)

	})

	convey.Convey("Bad Network: read big packet", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				if i == 2 {
					packetSize -= 1
				}
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = byte(i)

				payload := generateRandomBytes(int(packetSize))
				exceptPayload = append(exceptPayload, payload...)
				stumblingToWrite(t, server, append(header, payload...))
			}
		}()

		actualPayload, err := cm.Read()
		assert.Nil(t, err)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)

	})

	convey.Convey("Bad Network: read big packet, the last package size is equal to 16MB", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = byte(i)
				payload := generateRandomBytes(int(packetSize))
				exceptPayload = append(exceptPayload, payload...)
				stumblingToWrite(t, server, append(header, payload...))
			}
			header := make([]byte, 4)
			binary.LittleEndian.PutUint32(header[:4], 0)
			header[3] = byte(totalPackets)
			_, err := server.Write(header)
			assert.Nil(t, err)
		}()

		actualPayload, err := cm.Read()
		assert.Nil(t, err)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})
}

func TestMySQLProtocolWriteRows(t *testing.T) {
	var err error
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(newLeakCheckAllocator())
	convey.Convey("test write packet", t, func() {
		rows := 20
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, err := NewIOSession(client, pu)
		assert.Nil(t, err)
		cReader, err := NewIOSession(server, pu)
		assert.Nil(t, err)
		cReader.allowedPacketSize = int(MaxPayloadSize) * 16
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		columns := rand.Intn(20) + 1
		fieldSize := rand.Intn(20) + 1
		go func() {
			var err error
			for i := 0; i < rows; i++ {
				exceptRow := make([]byte, 0)
				err = cWriter.BeginPacket()
				assert.Nil(t, err)
				for j := 0; j < columns; j++ {
					field := generateRandomBytes(fieldSize)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					assert.Nil(t, err)
				}
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				assert.Nil(t, err)
			}
			err = cWriter.Flush()
			assert.Nil(t, err)
		}()

		var data []byte
		for i := 0; i < rows; i++ {
			data, err = ReadPacketForTest(cReader)
			assert.Nil(t, err)
			actualPayload = append(actualPayload, data)
		}
		remain, err := hasData(server)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		convey.So(remain, convey.ShouldBeFalse)

	})

	convey.Convey("test write packet when row size > 1MB", t, func() {
		rows := 2
		convey.Convey("many columns", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 1024
			fieldSize := 4 * 1024
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)
				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})
		convey.Convey("big field size", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := 1024 * 1024 * 2
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)

				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})
	})

	convey.Convey("test write packet when sometime buffer size >= 16MB", t, func() {
		rows := 1
		convey.Convey("big field size", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := 1024 * 1024 * 20
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)

				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})

		convey.Convey("big columns number", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 1024
			fieldSize := 1024 * 20
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)
				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})

		convey.Convey("row size equal to 16MB", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := int(MaxPayloadSize / 2)
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}

					field := generateRandomBytes(1)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					assert.Nil(t, err)
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)
				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})

		convey.Convey("field size equal to 16MB", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			assert.Nil(t, err)
			cReader, err := NewIOSession(server, pu)
			assert.Nil(t, err)
			cReader.allowedPacketSize = int(MaxPayloadSize) * 16
			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := int(MaxPayloadSize)
			go func() {
				var err error
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					assert.Nil(t, err)
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						assert.Nil(t, err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					assert.Nil(t, err)
				}
				err = cWriter.Flush()
				assert.Nil(t, err)
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				assert.Nil(t, err)
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
		})
	})

}

func TestMySQLBufferReadLoadLocal(t *testing.T) {
	var err error
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(newLeakCheckAllocator())
	convey.Convey("test read load local packet", t, func() {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, _ := NewIOSession(client, pu)
		assert.Nil(t, err)
		cReader, _ := NewIOSession(server, pu)
		assert.Nil(t, err)
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		go func() {
			var err error
			fieldSizes := []int{1000, 2000, 1000}
			for i := 0; i < 3; i++ {
				exceptRow := make([]byte, 0)
				err = cWriter.BeginPacket()
				assert.Nil(t, err)
				field := generateRandomBytes(fieldSizes[i])
				exceptRow = append(exceptRow, field...)
				err = cWriter.Append(field...)
				assert.Nil(t, err)
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				assert.Nil(t, err)
			}
			err = cWriter.Flush()
			assert.Nil(t, err)
		}()

		var err error
		var data []byte
		for i := 0; i < 3; i++ {
			data, err = cReader.ReadLoadLocalPacket()
			payload := make([]byte, len(data))
			copy(payload, data)
			assert.Nil(t, err)
			actualPayload = append(actualPayload, payload)
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
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(newLeakCheckAllocator())
	convey.Convey("test read max allowed packet", t, func() {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, err := NewIOSession(client, pu)
		assert.Nil(t, err)
		cReader, err := NewIOSession(server, pu)
		assert.Nil(t, err)
		ses := &Session{}
		ses.respr = &MysqlResp{
			mysqlRrWr: &MysqlProtocolImpl{io: NewIOPackage(true), tcpConn: cReader},
		}
		cReader.ses = ses
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		go func() {
			for {
				_, err := ReadPacketForTest(cWriter)
				if err != nil {
					return
				}
			}
		}()
		go func() {
			var err error

			err = cWriter.BeginPacket()
			assert.Nil(t, err)
			exceptRow := generateRandomBytes(int(MaxPayloadSize) / 2)
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			assert.Nil(t, err)
			err = cWriter.FinishedPacket()
			assert.Nil(t, err)

			err = cWriter.BeginPacket()
			assert.Nil(t, err)
			exceptRow = generateRandomBytes(int(MaxPayloadSize) - 1)
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			assert.Nil(t, err)
			err = cWriter.FinishedPacket()
			assert.Nil(t, err)

			err = cWriter.BeginPacket()
			assert.Nil(t, err)
			exceptRow = generateRandomBytes(int(MaxPayloadSize) * 2)
			exceptPayload = append(exceptPayload, exceptRow)
			err = cWriter.Append(exceptRow...)
			assert.Nil(t, err)
			err = cWriter.FinishedPacket()
			assert.Nil(t, err)
		}()

		var data []byte
		data, err = ReadPacketForTest(cReader)
		convey.So(err, convey.ShouldBeNil)
		actualPayload = append(actualPayload, data)
		data, err = ReadPacketForTest(cReader)
		convey.So(err, convey.ShouldBeNil)
		actualPayload = append(actualPayload, data)
		_, err = ReadPacketForTest(cReader)
		convey.So(err, convey.ShouldNotBeNil)
		for remain, _ = hasData(server); remain; remain, _ = hasData(server) {
			_, _ = cReader.conn.Read(make([]byte, int(MaxPayloadSize)))
		}
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload[:2]), convey.ShouldBeTrue)

	})
}

var _ Allocator = new(leakCheckAllocator)

const (
	leakCheckAllocatorModeNormal = iota
	leakCheckAllocatorModeAllocReturnErr
	leakCheckAllocatorModeAllocPanic
)

type leakCheckAllocator struct {
	sync.Mutex
	allocated uint64
	freed     uint64
	records   map[unsafe.Pointer]int
	mod       int
}

func newLeakCheckAllocator() *leakCheckAllocator {
	return &leakCheckAllocator{
		records: make(map[unsafe.Pointer]int),
	}
}

func (lca *leakCheckAllocator) Alloc(capacity int) ([]byte, error) {
	lca.Lock()
	defer lca.Unlock()
	if lca.mod == leakCheckAllocatorModeAllocReturnErr {
		return nil, moerr.NewInternalErrorNoCtx("leak check allocator returns eror")
	} else if lca.mod == leakCheckAllocatorModeAllocPanic {
		panic("leak check allocator panic")
	}
	buf := make([]byte, capacity)
	lca.allocated += uint64(len(buf))
	lca.records[unsafe.Pointer(&buf[0])] = capacity
	return buf, nil
}

func (lca *leakCheckAllocator) Free(bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	lca.Lock()
	defer lca.Unlock()
	if _, ok := lca.records[unsafe.Pointer(&bytes[0])]; ok {
		delete(lca.records, unsafe.Pointer(&bytes[0]))
	} else {
		panic(fmt.Sprintf("no such ptr %v", unsafe.Pointer(&bytes[0])))
	}
	lca.freed += uint64(len(bytes))
}

func (lca *leakCheckAllocator) CheckBalance() bool {
	lca.Lock()
	defer lca.Unlock()
	return lca.allocated == lca.freed && len(lca.records) == 0
}

func Test_ListBlock(t *testing.T) {
	const n = 1024
	leakAlloc := newLeakCheckAllocator()
	buf, err := leakAlloc.Alloc(n)
	assert.Nil(t, err)
	mem1 := MemBlock{
		data: buf,
	}
	assert.Equal(t, mem1.BufferLen(), n)
	mem1.ResetIndices()

	src1 := []byte("test mem block")
	mem1.CopyDataIn(src1)
	mem1.IncWriteIndex(len(src1))
	assert.False(t, mem1.IsFull())
	assert.Equal(t, mem1.AvailableDataLen(), len(src1))
	assert.Equal(t, mem1.AvailableData(), src1)
	assert.Equal(t, mem1.Head(), src1[:HeaderLengthOfTheProtocol])
	assert.Equal(t, mem1.AvailableDataAfterHead(), src1[HeaderLengthOfTheProtocol:])
	assert.Equal(t, mem1.AvailableSpaceLen(), n-mem1.AvailableDataLen())
	dst1 := make([]byte, n)
	mem1.CopyDataAfterHeadOutTo(dst1)
	assert.Equal(t, dst1[:len(src1)-HeaderLengthOfTheProtocol], src1[HeaderLengthOfTheProtocol:])
	mem1.IncReadIndex(HeaderLengthOfTheProtocol)
	mem1.Adjust()
	assert.Equal(t, mem1.AvailableDataLen(), len(src1)-HeaderLengthOfTheProtocol)
	assert.True(t, bytes.Equal(mem1.AvailableData(), src1[HeaderLengthOfTheProtocol:]))

	mem1.freeBuffUnsafe(leakAlloc)
	assert.Equal(t, mem1.BufferLen(), 0)
	assert.True(t, leakAlloc.CheckBalance())

	var mem2 *MemBlock
	mem2.freeBuffUnsafe(leakAlloc)
}

func Test_NewIOSessionFailed(t *testing.T) {
	var err error
	var conn *Conn
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	aAlloc := newLeakCheckAllocator()
	aAlloc.mod = leakCheckAllocatorModeAllocReturnErr
	setGlobalSessionAlloc(aAlloc)
	conn, err = NewIOSession(client, pu)
	assert.NotNil(t, err)
	assert.Nil(t, conn)
	assert.Zero(t, aAlloc.allocated)

	aAlloc.mod = 0
	conn, err = NewIOSession(client, pu)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	assert.NotNil(t, conn.fixBuf.data)
	assert.Equal(t, uint64(fixBufferSize), aAlloc.allocated)

	conn2 := conn.RawConn()
	conn.UseConn(conn2)
	conn.RawConn()
}

func testDefer1(t *testing.T) {
	var err error
	a := 5
	defer func(inputErr error) {
		assert.Nil(t, inputErr)
	}(err)

	defer func() {
		assert.NotNil(t, err)
	}()

	defer func(inputA int) {
		assert.Equal(t, 5, inputA)
	}(a)

	a = 10

	err = moerr.NewInternalErrorNoCtx("err is not nil")

	fmt.Println(a)
}

func Test_defer(t *testing.T) {
	testDefer1(t)
}

func Test_ConnClose(t *testing.T) {
	tConn := &testConn{
		mod: 1,
	}
	conn := &Conn{
		conn: tConn,
	}
	err := conn.Close()
	assert.NotNil(t, err)
}

func TestConn_CheckAllowedPacketSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.respr = &MysqlResp{
		mysqlRrWr: &testMysqlWriter{mod: 1},
	}

	conn := &Conn{
		ses: ses,
	}
	err := conn.CheckAllowedPacketSize(5)
	assert.NotNil(t, err)
}

func makeHead(dlen int, seq uint8) []byte {
	var head [4]byte
	binary.LittleEndian.PutUint32(head[:], uint32(dlen))
	head[3] = seq
	return head[:]
}

func makePacket(data []byte, seq uint8) []byte {
	return append(makeHead(len(data), seq), data...)
}

func newTestConn(t *testing.T, leakAlloc *leakCheckAllocator) (*testConn, *Conn) {
	var conn *Conn
	tConn := &testConn{}
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	assert.Nil(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setGlobalSessionAlloc(leakAlloc)
	conn, err = NewIOSession(tConn, pu)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	return tConn, conn
}

func TestConn_ReadLoadLocalPacketErr(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn
	var read []byte
	var tConn *testConn
	payload1Len := 10

	payload1 := make([]byte, payload1Len)
	for i := 0; i < payload1Len; i++ {
		payload1[i] = byte(i)
	}

	payload2Len := 20
	payload2 := make([]byte, payload2Len)
	for i := 0; i < payload2Len; i++ {
		payload2[i] = byte(1)
	}

	tConn, conn = newTestConn(t, leakAlloc)

	resetFunc := func() {
		leakAlloc.mod = 0
		tConn.mod = 0
		tConn.data = nil
	}

	for loop := 0; loop < 2; loop++ {
		{
			resetFunc()
			tConn.mod = testConnModSetReadDeadlineReturnErr
			conn.timeout = time.Second * 2
			_, _ = tConn.Write(makePacket(payload1, 1))
			read, err = conn.ReadLoadLocalPacket()
			assert.NotNil(t, err)
			assert.Nil(t, read)
			assert.True(t, !leakAlloc.CheckBalance())
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//allocate mem first failed
			resetFunc()
			leakAlloc.mod = leakCheckAllocatorModeAllocReturnErr
			_, _ = tConn.Write(makePacket(payload1, 1))
			read, err = conn.ReadLoadLocalPacket()
			assert.NotNil(t, err)
			assert.Nil(t, read)
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//allocate mem first success, second failed.
			resetFunc()
			_, _ = tConn.Write(makePacket(payload1, 2))
			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				if i == 1 {
					leakAlloc.mod = leakCheckAllocatorModeAllocReturnErr
				}
				read, err = conn.ReadLoadLocalPacket()
				if i == 0 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(read, payload1))
				} else if i == 1 {
					assert.NotNil(t, err)
					assert.Nil(t, read)
					assert.Nil(t, conn.loadLocalBuf.data)
				}

			}
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//allocate mem first success, second failed.
			resetFunc()
			_, _ = tConn.Write(makePacket(payload1, 2))

			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				if i == 1 {
					tConn.mod = testConnModReadReturnErr
				}
				read, err = conn.ReadLoadLocalPacket()
				if i == 0 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(read, payload1))
				} else if i == 1 {
					assert.NotNil(t, err)
					assert.Nil(t, read)
					assert.Nil(t, conn.loadLocalBuf.data)
				}

			}

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//panic recover release memory
			//first allocate mem panic
			resetFunc()
			_, _ = tConn.Write(makePacket(payload1, 2))

			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				if i == 0 {
					leakAlloc.mod = leakCheckAllocatorModeAllocPanic
				} else {
					leakAlloc.mod = 0
				}
				read, err = conn.ReadLoadLocalPacket()
				if i == 0 {
					assert.NotNil(t, err)
					assert.Nil(t, read)
					assert.Nil(t, conn.loadLocalBuf.data)
					//skip payload1 data
					buf := make([]byte, payload1Len)
					_, _ = tConn.Read(buf)
				} else if i == 1 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(payload2, read))
					assert.NotNil(t, conn.loadLocalBuf.data)
				}

			}
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//panic recover release memory
			//second allocate mem panic
			resetFunc()
			_, _ = tConn.Write(makePacket(payload1, 2))

			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				if i == 1 {
					leakAlloc.mod = leakCheckAllocatorModeAllocPanic
				}
				read, err = conn.ReadLoadLocalPacket()
				if i == 0 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(payload1, read))
					assert.NotNil(t, conn.loadLocalBuf.data)
				} else if i == 1 {
					assert.NotNil(t, err)
					assert.Nil(t, read)
					assert.Nil(t, conn.loadLocalBuf.data)
				}

			}
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//panic recover release memory
			//read second packet panic
			resetFunc()
			_, _ = tConn.Write(makePacket(payload1, 2))

			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				if i == 1 {
					tConn.mod = testConnModReadPanic
				}
				read, err = conn.ReadLoadLocalPacket()
				if i == 0 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(payload1, read))
					assert.NotNil(t, conn.loadLocalBuf.data)
				} else if i == 1 {
					assert.NotNil(t, err)
					assert.Nil(t, read)
					assert.Nil(t, conn.loadLocalBuf.data)
				}

			}
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//allocate mem both success
			resetFunc()

			_, _ = tConn.Write(makePacket(payload1, 2))
			_, _ = tConn.Write(makePacket(payload2, 3))

			for i := 0; i < 2; i++ {
				read, err = conn.ReadLoadLocalPacket()

				if i == 0 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(read, payload1))
				} else if i == 1 {
					assert.Nil(t, err)
					assert.True(t, bytes.Equal(read, payload2))
				}

			}
			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}
	}

	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func TestConn_ReadErr(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn
	var tConn *testConn
	var read []byte
	payload1Len := 10

	payload1 := make([]byte, payload1Len)
	for i := 0; i < payload1Len; i++ {
		payload1[i] = byte(i)
	}

	payload2Len := 20
	payload2 := make([]byte, payload2Len)
	for i := 0; i < payload2Len; i++ {
		payload2[i] = byte(1)
	}

	payload3Len := 2 * 1024 * 1024
	payload3 := make([]byte, payload3Len)
	xVal := uint8(0)
	for i := 0; i < payload3Len; i++ {
		payload3[i] = xVal
		xVal++
	}

	payload4Len := int(MaxPayloadSize)
	payload4 := make([]byte, payload4Len)
	xVal = 0
	for i := 0; i < payload4Len; i++ {
		payload4[i] = xVal
		xVal++
	}

	tConn, conn = newTestConn(t, leakAlloc)

	resetFunc := func() {
		leakAlloc.mod = 0
		tConn.mod = 0
		tConn.data = nil
	}
	runCaseFunc := func() {
		{
			resetFunc()
			conn.Reset()

			//success
			_, _ = tConn.Write(makePacket(payload1, 1))
			_, _ = tConn.Write(makePacket(payload2, 2))
			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload1, read))
			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			resetFunc()
			conn.Reset()
			//success
			_, _ = tConn.Write(makePacket(payload1, 1))
			_, _ = tConn.Write(makePacket(payload2, 2))
			_, _ = tConn.Write(makePacket(payload3, 3))
			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload1, read))
			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload2, read))
			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload3, read))
			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//success
			//test cases:
			//just 16MB
			//more than 16MB but less than 64MB
			//more than 64MB
			resetFunc()
			conn.Reset()

			//16MB
			_, _ = tConn.Write(makePacket(payload4, 4))
			_, _ = tConn.Write(makePacket([]byte{}, 5))
			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload4, read))

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//success
			//test cases:
			//more than 16MB but less than 64MB
			//more than 64MB
			resetFunc()
			conn.Reset()

			conn.allowedPacketSize = int(MaxPayloadSize) * 2

			//32MB
			_, _ = tConn.Write(makePacket(payload4, 4))
			_, _ = tConn.Write(makePacket(payload4, 5))
			_, _ = tConn.Write(makePacket([]byte{}, 6))
			read, err = conn.Read()
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload4, read[:MaxPayloadSize]))
			assert.True(t, bytes.Equal(payload4, read[MaxPayloadSize:]))

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			//success
			//test cases:
			//just 16MB
			//more than 16MB but less than 64MB
			//more than 64MB
			resetFunc()
			conn.Reset()

			conn.allowedPacketSize = int(MaxPayloadSize) * 3

			readCnt := 0
			stub1 := gostub.Stub(&ReadOnePayload,
				func(c *Conn, payloadLength int) ([]byte, error) {
					if readCnt == 1 {
						return nil, moerr.NewInternalErrorNoCtx("ReadNBytesIntoBuf returns err")
					}
					readCnt++
					return c.ReadOnePayload(payloadLength)
				})
			defer stub1.Reset()

			//32MB second read returns error
			_, _ = tConn.Write(makePacket(payload4, 4))
			_, _ = tConn.Write(makePacket(payload4, 5))
			_, _ = tConn.Write(makePacket(payload4, 6))
			_, _ = tConn.Write(makePacket([]byte{}, 7))
			read, err = conn.Read()
			assert.NotNil(t, err)
			assert.Nil(t, read)

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())
		}

		{
			//success
			//test cases:
			//just 16MB
			//more than 16MB but less than 64MB
			resetFunc()
			conn.Reset()

			conn.allowedPacketSize = int(MaxPayloadSize) * 3

			readCnt := 0

			stub1 := gostub.Stub(&ReadOnePayload,
				func(c *Conn, payloadLength int) ([]byte, error) {
					if readCnt == 1 {
						panic("ReadNBytesIntoBuf panics")
					}
					readCnt++
					return c.ReadOnePayload(payloadLength)
				})
			defer stub1.Reset()

			//32MB second read panic
			_, _ = tConn.Write(makePacket(payload4, 4))
			_, _ = tConn.Write(makePacket(payload4, 5))
			_, _ = tConn.Write(makePacket(payload4, 6))
			_, _ = tConn.Write(makePacket([]byte{}, 7))
			read, err = conn.Read()
			assert.NotNil(t, err)
			assert.Nil(t, read)

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())
		}
	}
	for loop := 0; loop < 2; loop++ {
		runCaseFunc()
	}
	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func TestConn_ReadOnePayload(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn
	var tConn *testConn
	var read []byte
	payload1Len := 10

	payload1 := make([]byte, payload1Len)
	for i := 0; i < payload1Len; i++ {
		payload1[i] = byte(i)
	}

	payload2Len := 20
	payload2 := make([]byte, payload2Len)
	for i := 0; i < payload2Len; i++ {
		payload2[i] = byte(1)
	}

	payload3Len := 2 * 1024 * 1024
	payload3 := make([]byte, payload3Len)
	xVal := uint8(0)
	for i := 0; i < payload3Len; i++ {
		payload3[i] = xVal
		xVal++
	}

	payload4Len := int(MaxPayloadSize)
	payload4 := make([]byte, payload4Len)
	xVal = 0
	for i := 0; i < payload4Len; i++ {
		payload4[i] = xVal
		xVal++
	}

	tConn, conn = newTestConn(t, leakAlloc)

	resetFunc := func() {
		leakAlloc.mod = 0
		tConn.mod = 0
		tConn.data = nil
	}

	runCaseFunc := func() {
		{
			resetFunc()
			conn.Reset()

			//success
			_, _ = tConn.Write(payload1)
			_, _ = tConn.Write(payload2)
			_, _ = tConn.Write([]byte{})
			read, err = conn.ReadOnePayload(payload1Len)
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload1, read))
			leakAlloc.Free(read)

			read, err = conn.ReadOnePayload(payload2Len)
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload2, read))
			leakAlloc.Free(read)

			read, err = conn.ReadOnePayload(0)
			assert.Nil(t, err)
			assert.Nil(t, read)
			leakAlloc.Free(read)

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())

			conn.freeNoFixBuffUnsafe()
			assert.True(t, !leakAlloc.CheckBalance())
		}

		{
			resetFunc()
			conn.Reset()

			readCnt := 0
			stub1 := gostub.Stub(&ReadNBytesIntoBuf,
				func(c *Conn, buf []byte, n int) error {
					if readCnt == 1 {
						return moerr.NewInternalErrorNoCtx("ReadNBytesIntoBuf returns err")
					}
					readCnt++
					return c.ReadNBytesIntoBuf(buf, n)
				})
			defer stub1.Reset()

			_, _ = tConn.Write(payload1)
			_, _ = tConn.Write(payload2)
			read, err = conn.ReadOnePayload(payload1Len)
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload1, read))
			leakAlloc.Free(read)

			read, err = conn.ReadOnePayload(payload2Len)
			assert.NotNil(t, err)
			assert.Nil(t, read)
			leakAlloc.Free(read)

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())
		}

		{
			resetFunc()
			conn.Reset()

			readCnt := 0

			stub1 := gostub.Stub(&ReadNBytesIntoBuf,
				func(c *Conn, buf []byte, n int) error {
					if readCnt == 1 {
						panic("ReadNBytesIntoBuf panics")
					}
					readCnt++
					return c.ReadNBytesIntoBuf(buf, n)
				})
			defer stub1.Reset()

			_, _ = tConn.Write(payload1)
			_, _ = tConn.Write(payload2)
			read, err = conn.ReadOnePayload(payload1Len)
			assert.Nil(t, err)
			assert.True(t, bytes.Equal(payload1, read))
			leakAlloc.Free(read)

			read, err = conn.ReadOnePayload(payload2Len)
			assert.NotNil(t, err)
			assert.Nil(t, read)
			leakAlloc.Free(read)

			assert.NotNil(t, conn.fixBuf.data)
			assert.Zero(t, conn.dynamicWrBuf.Len())
		}
	}
	for loop := 0; loop < 2; loop++ {
		runCaseFunc()
	}
	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func TestConn_AllocNewBlock(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn

	_, conn = newTestConn(t, leakAlloc)

	{
		err = conn.AllocNewBlock(10)
		assert.Nil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 1)
		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	{
		leakAlloc.mod = leakCheckAllocatorModeAllocReturnErr
		err = conn.AllocNewBlock(10)
		assert.NotNil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 0)
		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	{
		leakAlloc.mod = 0
		conn.dynamicWrBuf = nil
		err = conn.AllocNewBlock(10)
		assert.NotNil(t, err)
		assert.Nil(t, conn.dynamicWrBuf)
		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func Test_AppendPart(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn

	payload3Len := 2 * 1024 * 1024
	payload3 := make([]byte, payload3Len)
	xVal := uint8(0)
	for i := 0; i < payload3Len; i++ {
		payload3[i] = xVal
		xVal++
	}

	_, conn = newTestConn(t, leakAlloc)

	{
		err = conn.AppendPart(payload3)
		assert.Nil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 1)
		assert.Equal(t, conn.packetLength, payload3Len)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	{
		stub1 := gostub.Stub(&AllocNewBlock,
			func(c *Conn, _ int) error {
				return moerr.NewInternalErrorNoCtx("AllocNewBlock returns err")
			})

		oldPacketLen := conn.packetLength
		err = conn.AppendPart(payload3)
		assert.NotNil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 0)
		assert.Equal(t, conn.packetLength, oldPacketLen)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())

		stub1.Reset()
	}

	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func Test_Append(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn

	payload1Len := 10

	payload1 := make([]byte, payload1Len)
	for i := 0; i < payload1Len; i++ {
		payload1[i] = byte(i)
	}
	_, conn = newTestConn(t, leakAlloc)

	resetFunc := func() {
		leakAlloc.mod = 0
	}

	{
		resetFunc()
		conn.Reset()

		err = conn.Append(payload1...)
		assert.Nil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 0)
		assert.Equal(t, conn.packetLength, payload1Len)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	{
		resetFunc()
		conn.Reset()

		stub1 := gostub.Stub(&AppendPart,
			func(c *Conn, elems []byte) error {
				return moerr.NewInternalErrorNoCtx("AppendPart returns err")
			})

		err = conn.Append(payload1...)
		assert.NotNil(t, err)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())

		stub1.Reset()
	}

	{
		resetFunc()
		conn.Reset()

		conn.packetLength = int(MaxPayloadSize) - payload1Len

		stub1 := gostub.Stub(&FinishedPacket,
			func(c *Conn) error {
				return moerr.NewInternalErrorNoCtx("FinishedPacket returns err")
			})

		err = conn.Append(payload1...)
		assert.NotNil(t, err)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())

		stub1.Reset()
	}

	{
		resetFunc()
		conn.Reset()

		conn.packetLength = int(MaxPayloadSize) - payload1Len

		stub1 := gostub.Stub(&BeginPacket,
			func(c *Conn) error {
				return moerr.NewInternalErrorNoCtx("BeginPacket returns err")
			})

		err = conn.Append(payload1...)
		assert.NotNil(t, err)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())

		stub1.Reset()
	}
	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}

func Test_BeginPacket(t *testing.T) {
	leakAlloc := newLeakCheckAllocator()
	var err error
	var conn *Conn

	payload1Len := 10

	payload1 := make([]byte, payload1Len)
	for i := 0; i < payload1Len; i++ {
		payload1[i] = byte(i)
	}
	_, conn = newTestConn(t, leakAlloc)

	resetFunc := func() {
		leakAlloc.mod = 0
	}

	{
		resetFunc()
		conn.Reset()

		err = conn.BeginPacket()
		assert.Nil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 0)
		assert.Equal(t, conn.bufferLength, 4)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())
	}

	{
		resetFunc()
		conn.Reset()

		stub1 := gostub.Stub(&AllocNewBlock,
			func(c *Conn, _ int) error {
				return moerr.NewInternalErrorNoCtx("AllocNewBlock returns err")
			})

		conn.curBuf.writeIndex = int(fixBufferSize) - 3

		err = conn.BeginPacket()
		assert.NotNil(t, err)
		assert.Equal(t, conn.dynamicWrBuf.Len(), 0)
		assert.Equal(t, conn.bufferLength, 0)

		conn.freeNoFixBuffUnsafe()
		assert.True(t, !leakAlloc.CheckBalance())

		stub1.Reset()
	}

	_ = conn.Close()
	assert.True(t, leakAlloc.CheckBalance())
}
