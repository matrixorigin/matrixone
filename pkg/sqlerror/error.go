package sqlerror

func New(code string, cause string) error {
	return &SqlError{code, cause}
}
