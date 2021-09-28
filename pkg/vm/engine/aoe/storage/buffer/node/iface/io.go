package iface

type Reader interface {
	Load() error
}

type Writer interface {
	Flush() error
}

type Cleaner interface {
	Clean() error
}

type IOHandle interface {
	Reader
	Writer
	Cleaner
}

type DefaultIOHandle struct {
	Reader
	Writer
	Cleaner
}
