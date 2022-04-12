package txnif

import (
	"fmt"
	"io"
)

type TxnCmd interface {
	WriteTo(io.Writer) error
	ReadFrom(io.Reader) error
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetType() int16
	String() string
}

type CmdFactory func(int16) TxnCmd

var cmdFactories = map[int16]CmdFactory{}

func RegisterCmdFactory(cmdType int16, factory CmdFactory) {
	_, ok := cmdFactories[cmdType]
	if ok {
		panic(fmt.Sprintf("duplicate cmd type: %d", cmdType))
	}
	cmdFactories[cmdType] = factory
}

func GetCmdFactory(cmdType int16) (factory CmdFactory) {
	factory = cmdFactories[cmdType]
	if factory == nil {
		panic(fmt.Sprintf("no factory found for cmd: %d", cmdType))
	}
	return
}
