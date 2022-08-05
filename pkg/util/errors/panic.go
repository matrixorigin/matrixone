// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors

/*
// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, sv *settings.Values, r any, depth int) {
	panicErr := PanicAsError(depth+1, r)
	log.Ops.Shoutf(ctx, severity.ERROR, "a panic has occurred!\n%+v", panicErr)

	// In addition to informing the user, also report the details to telemetry.
	sendCrashReport(ctx, sv, panicErr, ReportTypePanic)

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	log.Flush()
}

// PanicAsError turns r into an error if it is not one already.
func PanicAsError(depth int, r any) error {
	if err, ok := r.(error); ok {
		return WithStackDepth(err, depth+1)
	}
	return NewWithDepthf(depth+1, "panic: %v", r)
}
*/
