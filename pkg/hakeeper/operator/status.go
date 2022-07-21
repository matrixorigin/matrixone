// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2021 Matrix Origin.
//
// Simplified the behavior and the interface of the status.

package operator

// OpStatus represents the status of an Operator.
type OpStatus = uint32

// Status list
const (
	// Status list

	// STARTED and not finished. Next status: { SUCCESS, CANCELED, EXPIRED}.
	STARTED OpStatus = iota

	// Followings are end status, i.e. no next status.

	// SUCCESS Finished successfully
	SUCCESS

	// CANCELED due to some reason
	CANCELED

	// EXPIRED waiting for too long
	EXPIRED

	// Status list end
	statusCount    // Total count of status
	firstEndStatus = SUCCESS
)

type transition [statusCount][statusCount]bool

// Valid status transition
var validTrans = transition{
	STARTED: {
		SUCCESS:  true,
		CANCELED: true,
		EXPIRED:  true,
	},
	SUCCESS:  {},
	CANCELED: {},
	EXPIRED:  {},
}

var statusString = [statusCount]string{
	STARTED:  "Started",
	SUCCESS:  "Success",
	CANCELED: "Canceled",

	EXPIRED: "Expired",
}

// IsEndStatus checks whether s is an end status.
func IsEndStatus(s OpStatus) bool {
	return firstEndStatus <= s && s < statusCount
}

// OpStatusToString converts Status to string.
func OpStatusToString(s OpStatus) string {
	if s < statusCount {
		return statusString[s]
	}
	return "Unknown"
}
