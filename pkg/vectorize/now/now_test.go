package now

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
)

func TestTruncateTime(t *testing.T) {
	now := time.Now()

	_, err := truncateTime(now, -1)
	assert.Error(t, err)
	_, err = truncateTime(now, 7)
	assert.Error(t, err)

	tdt, err := truncateTime(now, 3)
	assert.NoError(t, err)

	assert.NotEqual(t, now.Nanosecond(), tdt.Nanosecond())
	assert.Equal(t, tdt.Nanosecond()/1000000, now.Nanosecond()/1000000)
	assert.Equal(t, tdt.Nanosecond()/1000, (tdt.Nanosecond()/1000000)*1000)
}

// @todo fsp is useless because types.DateTime has not implemented fsp
func compareDateTimeWithFsp(gots time.Time, mots types.Datetime, fsp int64) bool {
	//t1 := gots.Truncate(time.Duration(math.Pow10(9-int(fsp))) * time.Nanosecond)

	myear, mmonth, mday, _ := mots.ToDate().Calendar(true)
	mhour, mmin, msec := mots.Clock()

	return myear == int32(gots.Year()) &&
		mmonth == uint8(gots.Month()) &&
		mday == uint8(gots.Day()) &&
		mhour == int8(gots.Hour()) &&
		mmin == int8(gots.Minute()) &&
		msec == int8(gots.Second())
}

func TestEvalNowWithFsp(t *testing.T) {

	cqTz, err := time.LoadLocation("Asia/Chongqing")
	assert.NoError(t, err)
	cyTz, err := time.LoadLocation("Canada/Yukon")
	assert.NoError(t, err)

	cqTs := time.Now().In(cqTz)
	cyTs := cqTs.In(cyTz)

	proc := &process.Process{}
	proc.Statement.SetStatementTime(cqTs)
	proc.Statement.SetTimeZone(cyTz)

	xs := []int64{0, 3, 6}
	rs := make([]types.Datetime, len(xs))
	rs, err = EvalNowWithFsp(proc, xs, rs)
	assert.NoError(t, err)

	for i := 0; i < len(xs); i++ {
		assert.True(t, compareDateTimeWithFsp(cyTs, rs[i], xs[i]))
	}
}
