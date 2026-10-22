//go:build darwin

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package m0admission

import "golang.org/x/sys/unix"

const missingDataXattr = unix.ENOATTR
