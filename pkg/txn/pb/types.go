package pb

import (
	"math"
	"time"
)

// DN dn
// TODO: maybe remove to other package
type DN struct {
}

// TxnMeta transaction metadata
type TxnMeta struct {
	// ID transction id
	ID []byte
	// Status txn status
	Status TxnStatus
	// SnapshotTimestamp txn snapshot timestamp, determines the version of data
	// visible to the transaction. Initialized at the CN node and will not be
	// modified for the life of the transaction.
	SnapshotTimestamp Timestamp
}

type TxnStatus struct {
}

type DNOpRequest struct {
}

type DNOpResponse struct {
}

type TxnError struct {
}

// Timestamp is a HLC time value. All its field should never be accessed
// directly by its users.
type Timestamp struct {
	// PhysicalTime is the physical component of the HLC, it is read from a node's
	// wall clock time as Unix epoch time in nanoseconds. HLC requires this field
	// to be monotonically increase on each node.
	PhysicalTime int64 `protobuf:"varint,1,opt,name=physical_time,json=physicalTime,proto3" json:"physical_time,omitempty"`
	// LogicalTime is the logical component of the HLC, its value is maintained
	// according to the HLC algorithm. The HLC paper further establishes that its
	// value will not overflow in a real production environment.
	LogicalTime          uint32   `protobuf:"varint,2,opt,name=logical_time,json=logicalTime,proto3" json:"logical_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

// IsEmpty returns a boolean value indicating whether the current timestamp
// is an empty value.
func (m Timestamp) IsEmpty() bool {
	return m.PhysicalTime == 0 && m.LogicalTime == 0
}

// ToStdTime converts the HLC timestamp to a regular golang stdlib UTC
// timestamp. The logical time component of the HLC is lost after the
// conversion.
func (m Timestamp) ToStdTime() time.Time {
	return time.Unix(0, m.PhysicalTime).UTC()
}

// Equal returns a boolean value indicating whether the lhs timestamp equals
// to the rhs timestamp.
func (m Timestamp) Equal(rhs Timestamp) bool {
	return m.PhysicalTime == rhs.PhysicalTime &&
		m.LogicalTime == rhs.LogicalTime
}

// Less returns a boolean value indicating whether the lhs timestamp is less
// than the rhs timestamp value.
func (m Timestamp) Less(rhs Timestamp) bool {
	return m.PhysicalTime < rhs.PhysicalTime ||
		(m.PhysicalTime == rhs.PhysicalTime && m.LogicalTime < rhs.LogicalTime)
}

// Greater returns a boolean value indicating whether the lhs timestamp is
// greater than the rhs timestamp value.
func (m Timestamp) Greater(rhs Timestamp) bool {
	return m.PhysicalTime > rhs.PhysicalTime ||
		(m.PhysicalTime == rhs.PhysicalTime && m.LogicalTime > rhs.LogicalTime)
}

// LessEq returns a boolean value indicating whether the lhs timestamp is
// less than or equal to the rhs timestamp value.
func (m Timestamp) LessEq(rhs Timestamp) bool {
	return m.Less(rhs) || m.Equal(rhs)
}

// GreaterEq returns a boolean value indicating whether the lhs timestamp is
// greater than or equal to the rhs timestamp value.
func (m Timestamp) GreaterEq(rhs Timestamp) bool {
	return m.Greater(rhs) || m.Equal(rhs)
}

// Next returns the smallest timestamp that is greater than the current
// timestamp.
func (m Timestamp) Next() Timestamp {
	if m.LogicalTime == math.MaxUint32 {
		return Timestamp{PhysicalTime: m.PhysicalTime + 1}
	}

	return Timestamp{
		PhysicalTime: m.PhysicalTime,
		LogicalTime:  m.LogicalTime + 1,
	}
}

// Prev returns the smallest timestamp that is less than the current
// timestamp.
func (m Timestamp) Prev() Timestamp {
	if m.LogicalTime == 0 {
		return Timestamp{PhysicalTime: m.PhysicalTime - 1, LogicalTime: math.MaxUint32}
	}

	return Timestamp{
		PhysicalTime: m.PhysicalTime,
		LogicalTime:  m.LogicalTime - 1,
	}
}
