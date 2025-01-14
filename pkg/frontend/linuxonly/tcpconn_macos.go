//go:build darwin

package linuxonly

import (
	"sync"
)

func IsConnected(connMap *sync.Map) {
	return
}
