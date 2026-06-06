package datasync

import (
	"fmt"
	"io"
	"sync"
)

type progressLogger struct {
	mu sync.Mutex
	w  io.Writer
}

func newProgressLogger(w io.Writer) *progressLogger {
	return &progressLogger{w: w}
}

func (p *progressLogger) Printf(format string, args ...any) {
	if p == nil || p.w == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, format+"\n", args...)
}

func taskProgressLabel(task Task) string {
	return fmt.Sprintf("%s.%s.%s -> %s.%s",
		task.SourceName,
		task.SourceDatabase,
		task.SourceTable,
		task.TargetName,
		task.TargetDatabase,
	)
}
