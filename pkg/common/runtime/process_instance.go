// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");

package runtime

import "github.com/google/uuid"

var processInstanceID = uuid.NewString()

// ProcessInstanceID identifies this process incarnation, shared by every CN
// service hosted here. It is stable until process exit and changes on restart.
func ProcessInstanceID() string { return processInstanceID }
