package pi

import (
	"math"
)

var (
	GetPi func() float64
)

func init() {
	GetPi = getPiImpl
}

func getPiImpl() float64 {
	return math.Pi
}
