// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import "github.com/matrixorigin/matrixone/pkg/vm/process"

// closePipelineSpoolsAfterReadersStopped runs only at the joined attempt/RPC
// boundary. In particular, an inactive prepared Compile's Release must not
// collect another attempt's spools merely because it shares a BaseProcess.
func (c *Compile) closePipelineSpoolsAfterReadersStopped() {
	if c.importMemory != nil {
		for base := range c.importMemoryBases {
			base.ClosePipelineSpoolsAfterReadersStopped()
		}
		return
	}
	seen := make(map[*process.BaseProcess]bool)
	closeBase := func(proc *process.Process) {
		if proc == nil || proc.Base == nil || seen[proc.Base] || proc.Base.ImportMemory != nil {
			return
		}
		seen[proc.Base] = true
		proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	}
	closeBase(c.proc)
	visited := make(map[*Scope]bool)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil || visited[scope] {
			return
		}
		visited[scope] = true
		closeBase(scope.Proc)
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}
