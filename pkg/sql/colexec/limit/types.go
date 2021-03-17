package limit

type Argument struct {
	Seen  uint64 // seen is the number of tuples seen so far
	Limit uint64
}
