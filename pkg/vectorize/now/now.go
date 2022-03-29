package now

import (
	"fmt"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func truncateTime(t time.Time, fsp int64) (time.Time, error) {
	if fsp < 0 {
		return time.Time{}, fmt.Errorf("invalid fsp : %d, minimum is 0", fsp)
	} else if fsp > 6 {
		return time.Time{}, fmt.Errorf("invalid fsp : %d, maximum is 6", fsp)
	}
	return t.Truncate(time.Duration(math.Pow10(9-int(fsp))) * time.Nanosecond), nil
}

func EvalNowWithFsp(proc *process.Process, xs []int64, rs []types.Datetime) ([]types.Datetime, error) {
	// refer to : https://dev.mysql.com/doc/refman/8.0/en/datetime.html
	stmtTime := proc.Statement.GetStatementTime()
	// @todo matrixone doesn't treat time zone, so return a local timezone
	location := proc.Statement.GetTimeZone()

	for i, fsp := range xs {
		ts, err := truncateTime(stmtTime, fsp)
		if err != nil {
			return nil, err
		}

		ltime := ts.In(location)
		microsecond := ltime.Nanosecond() / 1000
		// @todo : types.DateTime hasn't yet implemented fsp(fractional seconds precision),
		// so, don't consider fsp when format DateTime to string.
		// Could place fsp with 3 bit to DateTime, but please consider whether DateTime is persisted to store
		rs[i] = types.FromClock(int32(ltime.Year()), uint8(ltime.Month()), uint8(ltime.Day()),
			uint8(ltime.Hour()), uint8(ltime.Minute()), uint8(ltime.Second()), uint32(microsecond))
	}
	return rs, nil
}
