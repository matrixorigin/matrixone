package limit

type Argument struct {
	Seen  int // seen is the number of tuples seen so far
	Limit int
}
