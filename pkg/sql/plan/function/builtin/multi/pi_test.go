package multi

import (
	"errors"
	"log"
	"math"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_Pi(t *testing.T) {
	convey.Convey("Test Pi function succ", t, func() {
		vec, err := Pi(nil, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(vec.Nsp.Np, convey.ShouldBeNil)
		data, ok := vec.Col.([]float64)
		if !ok {
			log.Fatal(errors.New("the Pi function return value type is not []float64"))
		}
		convey.So(data[0], convey.ShouldEqual, math.Pi)

	})
}
