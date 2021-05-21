package rpcserver

import (
	"fmt"
	"os"
	"testing"
	"time"

	"matrixone/pkg/logger"

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
	time.Sleep(time.Second)
}

func (h *hello) process(_ uint64, val interface{}, _ goetty.IOSession) error {
	fmt.Printf("%v: %s\n", h.cmd, val)
	return nil
}
