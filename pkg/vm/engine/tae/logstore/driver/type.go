package driver

type Driver interface {
	Append(*Entry) (lsn uint64)
	Truncate(lsn uint64) error
	GetTruncated() (lsn uint64)
	Read(lsn uint64) *Entry
	Close() error
}
