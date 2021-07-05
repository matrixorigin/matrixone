package sqlerror

import "fmt"

type SqlError struct {
	code  string
	cause string
}

var _ error = (*SqlError)(nil)

func (e *SqlError) Code() string  { return e.code }
func (e *SqlError) Error() string { return fmt.Sprintf("[%v]%v", e.code, e.cause) }
