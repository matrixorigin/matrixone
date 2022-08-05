package errors

import (
	"github.com/cockroachdb/errors/errutil"
	"github.com/cockroachdb/errors/secondary"
	"github.com/cockroachdb/errors/withstack"
)

func Wrap(err error, msg string) error {
	return errutil.WrapWithDepth(1, err, msg)
}

// Wrapf like cockroachdb
func Wrapf(err error, format string, args ...interface{}) error {
	return WrapWithDepthf(1, err, format, args...)
}

// WrapWithDepthf is like Wrapf except the depth to capture the stack
// trace is configurable.
// The the doc of `Wrapf()` for more details.
func WrapWithDepthf(depth int, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	var errRefs []error
	for _, a := range args {
		if e, ok := a.(error); ok {
			errRefs = append(errRefs, e)
		}
	}
	if format != "" || len(args) > 0 {
		err = errutil.WithMessagef(err, format, args...)
	}
	for _, e := range errRefs {
		err = secondary.WithSecondaryError(err, e)
	}
	err = withstack.WithStackDepth(err, depth+1)
	return err
}
