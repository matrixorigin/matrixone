package frontend

import "sync"

// migrateController is created in Routine and used to:
//  1. wait migration finished before close the routine.
//  2. check routine if closed before do the migration.
type migrateController struct {
	sync.Mutex
	migrateOnce sync.Once
	// closed indicates if the session has been closed.
	closed bool
	// inProgress indicates if the migration is in progress.
	inProgress bool
	// c is the channel which is used to wait for the migration
	// finished when close the routine.
	c chan struct{}
}

func newMigrateController() *migrateController {
	return &migrateController{
		closed:     false,
		inProgress: false,
		c:          make(chan struct{}, 1),
	}
}

// waitAndClose is called in the routine before the routine is cleaned up.
// if the migration is in progress, wait for it finished and set the closed to true.
func (mc *migrateController) waitAndClose() {
	mc.Lock()
	defer mc.Unlock()
	if mc.inProgress {
		<-mc.c
	}
	mc.closed = true
}

// beginMigrate is called before the migration started. It check if the routine
// has been closed.
func (mc *migrateController) beginMigrate() bool {
	mc.Lock()
	defer mc.Unlock()
	if mc.closed {
		return false
	}
	mc.inProgress = true
	return true
}

// endMigrate is called after the migration finished. It notifies the routine that
// it could clean up and set in progress to false.
func (mc *migrateController) endMigrate() {
	select {
	case mc.c <- struct{}{}:
	default:
	}
	mc.Lock()
	defer mc.Unlock()
	mc.inProgress = false
}
