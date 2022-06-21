package containers

func NewVector[T any](opts ...*Options) *Vector[T] {
	var v T
	_, ok := any(v).([]byte)
	if !ok {
		return &Vector[T]{
			Vector: NewStdVector[T](opts...),
		}
	}
	return &Vector[T]{
		Vector: NewStrVector[T](opts...),
	}
}
