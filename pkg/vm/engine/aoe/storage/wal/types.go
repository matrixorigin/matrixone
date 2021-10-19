package wal

type Payload interface {
	Marshal() ([]byte, error)
}

type Wal interface {
	Log(Payload) (*Entry, error)
	Checkpoint(interface{})
}
