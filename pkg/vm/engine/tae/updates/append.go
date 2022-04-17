package updates

import "sync"

type AppendNode struct {
	*sync.RWMutex
}
