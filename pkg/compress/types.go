package compress

import "fmt"

const (
	Lz4 = iota
)

type T uint8

func (t T) String() string {
	switch t {
	case Lz4:
		return "LZ4"
	}
	return fmt.Sprintf("unexpected compress type: %d", t)
}
