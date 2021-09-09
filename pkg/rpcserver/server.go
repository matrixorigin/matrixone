package rpcserver

import (
	"fmt"
	"go.uber.org/zap"
	"matrixone/pkg/rpcserver/message"

	"github.com/fagongzi/goetty"
)

func New(addr string, maxsize int, log *zap.Logger) (Server, error) {
	var err error

	s := new(server)
	encoder, decoder := NewCodec(maxsize)
	if s.app, err = goetty.NewTCPApplication(addr, s.onMessage,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder), goetty.WithLogger(log))); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *server) Stop() {
	s.app.Stop()
}

func (s *server) Run() error {
	return s.app.Start()
}

func (s *server) Register(f func(uint64, interface{}, goetty.IOSession) error) int {
	s.fs = append(s.fs, f)
	return len(s.fs)
}

func (s *server) onMessage(sess goetty.IOSession, value interface{}, seq uint64) error {
	m := value.(*message.Message)
	m.Sid = sess.ID()
	defer message.Release(m)
	if m.Cmd >= uint64(len(s.fs)) || s.fs[m.Cmd] == nil {
		return fmt.Errorf("unsupport command '%v'", m.Cmd)
	}
	return s.fs[m.Cmd](seq, value, sess)
}
