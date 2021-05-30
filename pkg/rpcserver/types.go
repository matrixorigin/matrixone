package rpcserver

import (
	"github.com/fagongzi/goetty"
)

type Server interface {
	Stop()
	Run() error
	Register(func(uint64, interface{}, goetty.IOSession) error) int
}

type server struct {
	app goetty.NetApplication
	fs  []func(uint64, interface{}, goetty.IOSession) error
}
