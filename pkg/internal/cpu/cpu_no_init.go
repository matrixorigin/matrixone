// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !386 && !amd64 && !amd64p32 && !arm && !arm64 && !ppc64 && !ppc64le && !s390x
// +build !386,!amd64,!amd64p32,!arm,!arm64,!ppc64,!ppc64le,!s390x

package cpu

func doinit() {
}
