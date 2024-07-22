package embed

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"go.uber.org/zap"
)

// Cluster is the mo cluster interface
type Cluster interface {
	Start() error
	Close() error
	GetService(sid string) (ServiceOperator, error)
}

type Option func(*cluster)

type ServiceOperator interface {
	ServiceID() string
	ServiceType() metadata.ServiceType
	Index() int
	Adjust(func(*ServiceConfig))
}

type operator struct {
	sync.RWMutex

	sid         string
	cfg         ServiceConfig
	serviceType metadata.ServiceType
	state       state

	reset struct {
		svc        service
		shutdownC  chan struct{}
		stopper    *stopper.Stopper
		rt         runtime.Runtime
		gossipNode *gossip.Node
		clock      clock.Clock
		logger     *zap.Logger
	}
}

type service interface {
	Start() error
	Close() error
}
