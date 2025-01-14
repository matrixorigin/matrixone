//go:build linux

package linuxonly

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"net"
	"reflect"
	"sync"
	"syscall"
)

const (
	TCP_ESTABLISHED = 1
	TCP_SYN_SENT    = 2
	TCP_SYN_RECV    = 3
	TCP_FIN_WAIT1   = 4
	TCP_FIN_WAIT2   = 5
	TCP_TIME_WAIT   = 6
	TCP_CLOSE       = 7
	TCP_CLOSE_WAIT  = 8
	TCP_LAST_ACK    = 9
	TCP_LISTEN      = 10
)

func GetsockoptTCPInfo(tcpConn *net.TCPConn, tcpInfo *syscall.TCPInfo) (uint8, error) {
	file, err := tcpConn.File()
	if err != nil {
		return 0, err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			logutil.Error("TCP info file close error", zap.Error(err))
			return
		}
	}()

	fd := file.Fd()
	size := reflect.TypeOf(*tcpInfo).Size()
	_, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.SOL_TCP, syscall.TCP_INFO,
		reflect.ValueOf(tcpInfo).Pointer(), reflect.ValueOf(&size).Pointer(), 0)
	if errno != 0 {
		return 0, errno
	}
	return tcpInfo.State, nil
}

func IsConnected(connMap *sync.Map) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err := moerr.ConvertPanicError(context.Background(), pErr)
			logutil.Error("panic in check Connection", zap.String("error", err.Error()))
		}
	}()

	tcpConnStatus := make(map[*net.TCPConn]uint8)
	tcpInfo := syscall.TCPInfo{}
	connMap.Range(func(key, value any) bool {
		tcpConn := key.(*net.TCPConn)
		tcpState, err := GetsockoptTCPInfo(tcpConn, &tcpInfo)
		if err != nil {
			logutil.Error("Failed to get TCP info", zap.Error(err))
			tcpConnStatus[tcpConn] = tcpState
			return true
		}
		switch tcpState {
		case TCP_LAST_ACK, TCP_CLOSE, TCP_FIN_WAIT1, TCP_FIN_WAIT2, TCP_TIME_WAIT:
			tcpConnStatus[tcpConn] = tcpState
			return true
		default:
			return true
		}
	})

	for key, value := range tcpConnStatus {
		cancel, ok := connMap.Load(key)
		if ok {
			TCPAddr := key.RemoteAddr().String()
			cancel.(context.CancelFunc)()
			connMap.Delete(key)
			switch value {
			case TCP_LAST_ACK:
				logutil.Infof("Connection %s is terminated, the status is TCP_LAST_ACK", TCPAddr)
			case TCP_CLOSE:
				logutil.Infof("Connection %s is terminated, the status is TCP_CLOSE", TCPAddr)
			case TCP_FIN_WAIT1:
				logutil.Infof("Connection %s is terminated, the status is TCP_FIN_WAIT1", TCPAddr)
			case TCP_FIN_WAIT2:
				logutil.Infof("Connection %s is terminated, the status is TCP_FIN_WAIT2", TCPAddr)
			case TCP_TIME_WAIT:
				logutil.Infof("Connection %s is terminated, the status is TCP_TIME_WAIT", TCPAddr)
			default:
				logutil.Infof("Connection %s is terminated", TCPAddr)
			}
		}
	}
}
