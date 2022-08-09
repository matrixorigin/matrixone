package frontend

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetTenantInfo(t *testing.T) {
	convey.Convey("tenant", t, func() {
		type input struct {
			input   string
			output  string
			wantErr bool
		}
		args := []input{
			{"u1", "sys:u1:public", false},
			{"tenant1:u1", "tenant1:u1:public", false},
			{"tenant1:u1:r1", "tenant1:u1:r1", false},
			{":u1:r1", "tenant1:u1:r1", true},
			{"tenant1:u1:", "tenant1:u1:r1", true},
			{"tenant1::r1", "tenant1::r1", true},
			{"tenant1:    :r1", "tenant1::r1", true},
			{"     : :r1", "tenant1::r1", true},
			{"   tenant1   :   u1   :   r1    ", "tenant1:u1:r1", false},
		}

		for _, arg := range args {
			ti, err := GetTenantInfo(arg.input)
			if arg.wantErr {
				convey.So(err, convey.ShouldNotBeNil)
			} else {
				convey.So(err, convey.ShouldBeNil)
				tis := ti.String()
				convey.So(tis, convey.ShouldEqual, arg.output)
			}
		}
	})
}
