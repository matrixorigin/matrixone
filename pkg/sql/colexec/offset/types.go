package offset

type Argument struct {
	Seen   uint64 // seen is the number of tuples seen so far
	Offset uint64
}
