package db

import "matrixone/pkg/vm/engine/aoe/storage/sched"

type metaHandler struct {
	sched.EventHandler
}

func newMetaHandler() *metaHandler {
	h := &metaHandler{
		EventHandler: sched.NewPoolHandler(4, nil),
	}
	return h
}
