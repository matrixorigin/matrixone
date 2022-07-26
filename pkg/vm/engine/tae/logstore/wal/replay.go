package wal

import (
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	// walEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

//truncate driver after replay(for batch store)
// func (w *WalImpl) Replay() {
// 	w.driver.Replay(func(e *entry.Entry) {
// 		w.replayEntry(e.Entry)
// 	})
// }

// func (w *WalImpl) replayEntry(e walEntry.Entry) {
// 	info := e.GetInfo().(*walEntry.Info)
// 	switch info.Group {
// 	case GroupInternal:
// 	case GroupCKP:
// 	case GroupC:
// 	}
// }
