package errors

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util"
)

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   message,
	}
	return &withStack{
		err,
		util.Callers(1),
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is called, and the format specifier.
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
	return &withStack{
		err,
		util.Callers(1),
	}
}

// WalkDeep does a depth-first traversal of all errors.
// The visitor function can return true to end the traversal early.
// In that case, WalkDeep will return true, otherwise false.
func WalkDeep(err error, visitor func(err error) bool) bool {
	// Go deep
	unErr := err
	for unErr != nil {
		if done := visitor(unErr); done {
			return true
		}
		unErr = Unwrap(unErr)
	}

	return false
}

/*
// NewWithDepthf creates an error with a formatted error message.
// A stack trace is retained, with depth.
func NewWithDepthf(depth int, format string, args ...interface{}) error {
	// If there's the verb %w in here, shortcut to fmt.Errorf()
	// and store the safe details as extra payload. That's
	// because we don't want to re-implement the error wrapping
	// logic from 'fmt' in there.
	var err error
	var errRefs []error
	for _, a := range args {
		if e, ok := a.(error); ok {
			errRefs = append(errRefs, e)
		}
	}
	redactable, wrappedErr := redact.HelperForErrorf(format, args...)
	if wrappedErr != nil {
		err = &withNewMessage{cause: wrappedErr, message: redactable}
	} else {
		err = &leafError{redactable}
	}
	for _, e := range errRefs {
		err = secondary.WithSecondaryError(err, e)
	}
	err = withstack.WithStackDepth(err, 1+depth)
	return err
}
*/
