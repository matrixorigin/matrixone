package frontend

import (
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
)

func TestMySQLProtocolRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	cm := NewIOSession(client, pu)
	convey.Convey("read big packet succ", t, func() {
		exceptPayload := make([]byte, 3*MaxPayloadSize)
		go func() {
			packetSize := MaxPayloadSize // 16MB
			seqID := byte(1)
			totalPackets := 3

			for i := 0; i < totalPackets; i++ {
				header := make([]byte, 4)
				binary.LittleEndian.PutUint32(header[:3], packetSize)
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

		defer client.Close()

		actualPayload, err := cm.Read()
		convey.So(err, convey.ShouldBeNil)
		convey.So(actualPayload, convey.ShouldEqual, exceptPayload)
	})
}
