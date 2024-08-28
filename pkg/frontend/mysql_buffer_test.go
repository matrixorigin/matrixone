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
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"

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
		err = c.ReadBytes(c.header[:], HeaderLengthOfTheProtocol)
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
			err = c.ReadBytes(signalPayload, totalLength)
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

func stumblingToWrite(conn net.Conn, packet []byte) {
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
		if err != nil {
			panic(err)
		}
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
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	cm, err := NewIOSession(client, pu)
	if err != nil {
		panic(err)
	}
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
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
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
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
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
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
				}
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
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
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
				}
			}
			header := make([]byte, 4)
			binary.LittleEndian.PutUint32(header[:4], 0)
			header[3] = byte(totalPackets)
			_, err := server.Write(header)
			if err != nil {
				panic(fmt.Sprintf("Failed to write header: %v", err))
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
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
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	cm, err := NewIOSession(client, pu)
	if err != nil {
		panic(err)
	}
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
				stumblingToWrite(server, append(header, payload...))
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
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
				stumblingToWrite(server, append(header, payload...))
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
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
				stumblingToWrite(server, append(header, payload...))
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
				}
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
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
				stumblingToWrite(server, append(header, payload...))
				if err != nil {
					panic(fmt.Sprintf("Failed to write payload: %v", err))
				}
			}
			header := make([]byte, 4)
			binary.LittleEndian.PutUint32(header[:4], 0)
			header[3] = byte(totalPackets)
			_, err := server.Write(header)
			if err != nil {
				panic(fmt.Sprintf("Failed to write header: %v", err))
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
	})
}

func TestMySQLProtocolWriteRows(t *testing.T) {
	var err error
	sv, err := getSystemVariables("test/system_vars_config.toml")
	sv.SessionTimeout.Duration = 5 * time.Minute
	if err != nil {
		panic(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)

	convey.Convey("test write packet", t, func() {
		rows := 20
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		cWriter, err := NewIOSession(client, pu)
		if err != nil {
			panic(err)
		}
		cReader, err := NewIOSession(server, pu)
		if err != nil {
			panic(err)
		}
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
				if err != nil {
					panic(fmt.Sprintf("Failed to begin packet: %v", err))
				}
				for j := 0; j < columns; j++ {
					field := generateRandomBytes(fieldSize)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					if err != nil {
						panic(fmt.Sprintf("Failed to append bytes: %v", err))
					}
				}
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				if err != nil {
					panic(fmt.Sprintf("Failed to finished packet: %v", err))
				}
			}
			err = cWriter.Flush()
			if err != nil {
				panic(fmt.Sprintf("Failed to flush packet: %v", err))
			}
		}()

		var data []byte
		for i := 0; i < rows; i++ {
			data, err = ReadPacketForTest(cReader)
			if err != nil {
				t.Fatalf("Failed to read packet: %v", err)
			}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}
				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}

				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}

				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}
				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}

					field := generateRandomBytes(1)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					if err != nil {
						panic(fmt.Sprintf("Failed to append bytes: %v", err))
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}
				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
			if err != nil {
				panic(err)
			}
			cReader, err := NewIOSession(server, pu)
			if err != nil {
				panic(err)
			}
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
					if err != nil {
						panic(fmt.Sprintf("Failed to begin packet: %v", err))
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							panic(fmt.Sprintf("Failed to append bytes: %v", err))
						}
					}
					exceptPayload = append(exceptPayload, exceptRow)
					err = cWriter.FinishedPacket()
					if err != nil {
						panic(fmt.Sprintf("Failed to finished packet: %v", err))
					}
				}
				err = cWriter.Flush()
				if err != nil {
					panic(fmt.Sprintf("Failed to flush packet: %v", err))
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = ReadPacketForTest(cReader)
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
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
		if err != nil {
			panic(err)
		}
		cReader, _ := NewIOSession(server, pu)
		if err != nil {
			panic(err)
		}
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
		cWriter, err := NewIOSession(client, pu)
		if err != nil {
			panic(err)
		}
		cReader, err := NewIOSession(server, pu)
		if err != nil {
			panic(err)
		}
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
			exceptRow = generateRandomBytes(int(MaxPayloadSize) - 1)
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
