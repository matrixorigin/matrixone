package frontend

import (
	"context"
	"github.com/google/uuid"
	"io"
	"sync"
	"time"
)

// BinlogSyncerConfig is the configuration for BinlogSyncer.
type BinlogSyncerConfig struct {
	// ServerID is the unique ID in cluster.
	ServerID uint32
	// Host is for MySQL server host.
	Host string
	// Port is for MySQL server port.
	Port uint16
	// User is for MySQL user.
	User string
	// Password is for MySQL password.
	Password string

	EventCacheCount int
}

type FormatDescriptionEvent struct {
}

type TableMapEvent struct {
}

type RowsEvent struct {
}

type GTIDEvent struct {
}

type BinlogParser struct {
	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent

	// for rawMode, we only parse FormatDescriptionEvent and RotateEvent
	rawMode bool

	parseTime               bool
	timestampStringLocation *time.Location

	// used to start/stop processing
	stopProcessing uint32

	useDecimal          bool
	ignoreJSONDecodeErr bool
	verifyChecksum      bool

	rowsEventDecodeFunc func(*RowsEvent, []byte) error

	tableMapOptionalMetaDecodeFunc func([]byte) error
}

// Position for binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}

// BinlogSyncer syncs binlog events from the server.
type BinlogSyncer struct {
	m sync.RWMutex

	cfg BinlogSyncerConfig

	c *client.Conn

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos Position

	prevGset, currGset GTIDSet

	// instead of GTIDSet.Clone, use this to speed up calculate prevGset
	prevMySQLGTIDEvent *GTIDEvent

	running bool

	ctx    context.Context
	cancel context.CancelFunc

	lastConnectionID uint32

	retryCount int
}

// Like MySQL GTID Interval struct, [start, stop), left closed and right open
// See MySQL rpl_gtid.h
type Interval struct {
	// The first GID of this interval.
	Start int64
	// The first GID after this interval.
	Stop int64
}

type UUIDSet struct {
	SID uuid.UUID

	Intervals []Interval
}

type GTIDSet struct {
	Sets map[string]*UUIDSet
}

type EventType byte

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

type Event interface {
	//Dump Event, format like python-mysql-replication
	Dump(w io.Writer)

	Decode(data []byte) error
}

type BinlogEvent struct {
	// raw binlog data which contains all data, including binlog header and event body, and including crc32 checksum if exists
	RawData []byte

	Header *EventHeader
	Event  Event
}

// BinlogStreamer gets the streaming event.
type BinlogStreamer struct {
	ch  chan *BinlogEvent
	ech chan error
	err error
}

func NewBinlogSyncer(cfg BinlogSyncerConfig) *BinlogSyncer {
	return &BinlogSyncer{}
}

func ParseGTIDSet(s string) (GTIDSet, error) {
	return GTIDSet{}, nil
}

func (b *BinlogSyncer) StartSyncGTID(gset GTIDSet) (*BinlogStreamer, error) {
	return &BinlogStreamer{}, nil
}

func (s *BinlogStreamer) GetEvent(ctx context.Context) (*BinlogEvent, error) {
	return &BinlogEvent{}, nil
}

func (e *BinlogEvent) Dump() {

}

func startReplication(ctx context.Context) {
	cfg := BinlogSyncerConfig{
		ServerID: 100,
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "snan",
		Password: "19990928",
	}
	syncer := NewBinlogSyncer(cfg)
	gtidSetStr := "78f4ca0e-ce5c-11ef-ac8e-000c2906c90a:1-27"
	gtidSet, _ := ParseGTIDSet(gtidSetStr)
	streamer, _ := syncer.StartSyncGTID(gtidSet)
	for {
		ev, _ := streamer.GetEvent(ctx)
		ev.Dump()
	}
}
