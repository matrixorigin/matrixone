// Copyright 2021 - 2023 Matrix Origin
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

//
// Poorman's usdt probe.
// Using bpftrace to trace go user space code is hard.  There are a few problems with go runtime
//  1. you simply cannot use uretprobe.  Go runtime may move stack around and return probe simply
//     does not work.
//  2. the argument to uprobe is rather hard to get.  Golang ABI changed so most of the stuff you
//     find on the web is obsolete.
//  3. there are ways of creating usdt with rather sophisticated bpf hacks, but I don't think it
//     can be understood by meer mortal.
//  4. thread id is not meaningful in go.  goroutine id is also fishy and rather hard to get.  Thread
//     id is extremely important/useful for tracing.
//
// Anyway, I didn't find a "satisfying" solution, so let's roll out our own.   It is extremely simple.
// When you need a static tracepoint, simply create a function here in this file.  The function should take
// *AT MOST* 9 arguments, *ALL* of them *int64*.  If you want to pass in a string, then use 2 params, a pointer
// that is converted to int64, and a length (you don't need this one if length is not needed).  The arguments
// are available in bpftrace in registers.  DO NOT pass in complex data structures as arg.  Go will put those
// on stack and it is simply impossible for bpftrace to decode.
//
// We suggest you always use first argument as a tag -- this is roughly the tid of C program.  You use this
// to track function call and return so that you won't mix call/return from different go routines.  A pointer
// of context, or go data structure converted to int64 usually works well.
//
// Not the noinline pragma. We will use bpftrace uprobe -- it can not be probed if the function is inlined.
//
// The following is here for reference.   See latest golang ABI for any changes.
//
// For AMD64, the register for args are
// 	ax, bx, cx, di, si, r8, r9, r10, r11
// Note that bpftrace does not use eax, rax, etc.   It is simply ax.   You can print out with %ld or %lx to get
// the full 8 bytes.
//
// For arm64, the registers are r0 - r15.
//

package moprobe

import (
	"context"
	gotrace "runtime/trace"
	"unsafe"
)

type RegionType int64

const (
	GenericRegion RegionType = iota
	PartitionStateHandleInsert
	PartitionStateHandleDel
	PartitionStateHandleMetaInsert
	SubscriptionPullLogTail
	RegionTypeMax
)

var regionTypeStr = [RegionTypeMax]string{
	"",
	"partiton state insert",
	"partiton state del",
	"partiton state metainsert",
	"subsciption pull logtail",
}

func WithRegion(ctx context.Context, rt RegionType, fn func()) {
	WithArgAndFn(ctx, rt, 0, 0, fn)
}

func WithArg(ctx context.Context, rt RegionType, arg1, arg2 int64) {
	WithArgAndFn(ctx, rt, arg1, arg2, nil)
}

func WithArgAndFn(ctx context.Context, rt RegionType, arg1, arg2 int64, fn func()) {
	tag := uintptr(unsafe.Pointer(&ctx))
	withTwoArg(tag, rt, arg1, arg2)
	if fn != nil {
		gotrace.WithRegion(ctx, regionTypeStr[rt], fn)
		withTwoArgRet(tag, rt, arg1, arg2)
	}
}

//go:noinline
func withTwoArg(tag uintptr, rt RegionType, v1, v2 int64) {
}

//go:noinline
func withTwoArgRet(tag uintptr, rt RegionType, v1, v2 int64) {
}
