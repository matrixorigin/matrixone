package compress

import "fmt"

const (
	None = iota
	Lz4
)

type T uint8

func (t T) String() string {
	switch t {
	case None:
		return "None"
	case Lz4:
		return "LZ4"
	}
	return fmt.Sprintf("unexpected compress type: %d", t)
}
