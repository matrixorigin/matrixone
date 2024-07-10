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
	for i := 0; i < n; i++ {
		data[i] = byte(rand.Intn(100) + 1)
	}
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
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
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
		for _, payload := range actualPayload {
			cm.allocator.Free(payload)
		}
	})

	convey.Convey("read small packet > 1MB", t, func() {
		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		repeat := 5
		packetSize := 1024 * 1024 * 5 // 16MB
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
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
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
		for _, payload := range actualPayload {
			cm.allocator.Free(payload)
		}

	})

	convey.Convey("read big packet", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			seqID := byte(1)
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				if i == 2 {
					packetSize -= 1
				}
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = seqID

				payload := make([]byte, packetSize)
				for j := range payload {
					payload[j] = byte(i)
				}
				exceptPayload = append(exceptPayload, payload...)
				_, err := server.Write(header)
				if err != nil {
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
				}
				seqID++
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		cm.allocator.Free(actualPayload)

	})

	convey.Convey("read big packet, the last package size is equal to 16MB", t, func() {
		exceptPayload := make([]byte, 0)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			seqID := byte(1)
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header[:4], packetSize)
				header[3] = seqID
				seqID += 1
				payload := make([]byte, packetSize)
				for j := range payload {
					payload[j] = byte(i)
				}
				exceptPayload = append(exceptPayload, payload...)
				_, err := server.Write(header)
				if err != nil {
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
				}

				seqID++
			}
			header := make([]byte, 4)
			binary.LittleEndian.PutUint32(header[:4], 0)
			header[3] = seqID
			seqID += 1
			_, err := server.Write(header)
			if err != nil {
				t.Fatalf("Failed to write header: %v", err)
			}
		}()

		actualPayload, err := cm.Read()
		if err != nil {
			t.Fatalf("Failed to read payload: %v", err)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		cm.allocator.Free(actualPayload)
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
		cReader, err := NewIOSession(server, pu)

		exceptPayload := make([][]byte, 0)
		actualPayload := make([][]byte, 0)
		columns := rand.Intn(20) + 1
		fieldSize := rand.Intn(20) + 1
		go func() {
			for i := 0; i < rows; i++ {
				exceptRow := make([]byte, 0)
				err = cWriter.BeginPacket()
				if err != nil {
					t.Fatalf("Failed to begin packet: %v", err)
				}
				for j := 0; j < columns; j++ {
					field := generateRandomBytes(fieldSize)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					if err != nil {
						t.Fatalf("Failed to append bytes: %v", err)
					}
				}
				exceptPayload = append(exceptPayload, exceptRow)
				err = cWriter.FinishedPacket()
				if err != nil {
					t.Fatalf("Failed to finished packet: %v", err)
				}
			}
			err = cWriter.Flush()
			if err != nil {
				t.Fatalf("Failed to flush packet: %v", err)
			}
		}()

		var data []byte
		for i := 0; i < rows; i++ {
			data, err = cReader.Read()
			if err != nil {
				t.Fatalf("Failed to read packet: %v", err)
			}
			actualPayload = append(actualPayload, data)
		}
		remain, err := hasData(server)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
		convey.So(remain, convey.ShouldBeFalse)
		for _, payload := range actualPayload {
			cReader.allocator.Free(payload)
		}

	})

	convey.Convey("test write packet when row size > 1MB", t, func() {
		rows := 2
		convey.Convey("many columns", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 1024 * 1024 * 2
			fieldSize := 2
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}
					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})
		convey.Convey("big field size", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := 1024 * 1024 * 2
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}
					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})
	})

	convey.Convey("test write packet when sometime buffer size >= 16MB", t, func() {
		rows := 3
		convey.Convey("big field size", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := 1024 * 1024 * 20
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}
					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})

		convey.Convey("big columns number", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 1024 * 1024 * 20
			fieldSize := 2
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}
					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})

		convey.Convey("row size equal to 16MB", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := int(MaxPayloadSize / 2)
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}

					field := generateRandomBytes(1)
					exceptRow = append(exceptRow, field...)
					err = cWriter.Append(field...)
					if err != nil {
						t.Fatalf("Failed to append bytes: %v", err)
					}

					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})

		convey.Convey("field size equal to 16MB", func() {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			cWriter, err := NewIOSession(client, pu)
			cReader, err := NewIOSession(server, pu)

			exceptPayload := make([][]byte, 0)
			actualPayload := make([][]byte, 0)
			columns := 2
			fieldSize := int(MaxPayloadSize)
			go func() {
				for i := 0; i < rows; i++ {
					exceptRow := make([]byte, 0)
					err = cWriter.BeginPacket()
					if err != nil {
						t.Fatalf("Failed to begin packet: %v", err)
					}
					for j := 0; j < columns; j++ {
						field := generateRandomBytes(fieldSize)
						exceptRow = append(exceptRow, field...)
						err = cWriter.Append(field...)
						if err != nil {
							t.Fatalf("Failed to append bytes: %v", err)
						}
					}

					err = cWriter.FinishedPacket()
					if err != nil {
						t.Fatalf("Failed to finished packet: %v", err)
					}
					exceptPayload = append(exceptPayload, exceptRow)
				}
				err = cWriter.Flush()
				if err != nil {
					t.Fatalf("Failed to flush packet: %v", err)
				}
			}()
			var data []byte

			for i := 0; i < rows; i++ {
				data, err = cReader.Read()
				if err != nil {
					t.Fatalf("Failed to read packet: %v", err)
				}
				actualPayload = append(actualPayload, data)
			}
			remain, err := hasData(server)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(actualPayload, exceptPayload), convey.ShouldBeTrue)
			convey.So(remain, convey.ShouldBeFalse)
			for _, payload := range actualPayload {
				cReader.allocator.Free(payload)
			}
		})
	})

}
