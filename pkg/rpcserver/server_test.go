package rpcserver

import (
	"os"
	"testing"
	"time"

	"matrixone/pkg/logger"
	"matrixone/pkg/rpcserver/message"

	"github.com/fagongzi/goetty"
)

type hello struct {
	cmd int
}

func TestServer(t *testing.T) {
	log := logger.New(os.Stderr, "hello:")
	log.SetLevel(logger.WARN)
	srv, err := New("127.0.0.1:8080", 1<<30, log)
	if err != nil {
		log.Fatal(err)
	}
	h := new(hello)
	h.cmd = srv.Register(h.process)
	srv.Run()
	time.Sleep(10 * time.Second)
}

func (h *hello) process(_ uint64, val interface{}, conn goetty.IOSession) error {
	for i := 0; i < 10; i++ {
		conn.WriteAndFlush(&message.Message{
			Data: val.(*message.Message).Data,
		})
	}
	return nil
}
