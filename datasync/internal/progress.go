package datasync

import (
	"fmt"
	"io"
	"sync"
	"time"
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
	message := fmt.Sprintf(format, args...)
	fmt.Fprintf(p.w, "%s %s\n", time.Now().Format("2006-01-02 15:04:05"), message)
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
