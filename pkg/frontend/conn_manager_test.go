package frontend

import (
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
	"time"
)

func TestMySQLProtocolRead(t *testing.T) {
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
	convey.Convey("read big packet succ", t, func() {
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

				_, err := server.Write(header)
				if err != nil {
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
				}
				exceptPayload = append(exceptPayload, payload...)
				seqID++
			}
		}()

		actualPayload, err := cm.Read()
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(actualPayload), convey.ShouldEqual, len(exceptPayload))
	})

	convey.Convey("read big packet succ, the last package is equal to 16MB", t, func() {
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

				_, err := server.Write(header)
				if err != nil {
					t.Fatalf("Failed to write header: %v", err)
				}

				_, err = server.Write(payload)
				if err != nil {
					t.Fatalf("Failed to write payload: %v", err)
				}
				exceptPayload = append(exceptPayload, payload...)
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
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(actualPayload), convey.ShouldEqual, len(exceptPayload))
	})
}
