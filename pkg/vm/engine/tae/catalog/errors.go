package catalog

import "errors"

var (
	ErrNotFound   = errors.New("tae catalog: not found")
	ErrDuplicate  = errors.New("tae catalog: duplicate")
	ErrCheckpoint = errors.New("tae catalog: checkpoint")

	ErrValidation = errors.New("tae catalog: validataion")

	ErrStopCurrRecur = errors.New("tae catalog: stop current recursion")
)
