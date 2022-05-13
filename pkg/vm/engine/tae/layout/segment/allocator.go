package segment

type Allocator struct {
	Create func(int32) error
}
