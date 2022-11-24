// Copyright 2021 - 2022 Matrix Origin
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

// Package log is used to control the log printing in MatrixOne.
//
// Printing logs using the Log package enables the following functions:
//  1. Uniform logger name
//  2. Support search all logs by service's uuid.
//  3. Support search the operation logs of all related processes by process ID, e.g.
//     search all logs of that transaction by transaction.
//  4. Sampling and printing of frequently output logs.
//  5. One line of code to add a time consuming log to a function.
//  6. Support for using the mo_ctl function to control log printing.
package log
