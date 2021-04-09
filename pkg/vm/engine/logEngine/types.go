package logEngine

import (
	"matrixone/pkg/vm/engine/logEngine/meta"
	"matrixone/pkg/vm/engine/logEngine/meta/client"
)

type LogEngine struct {
	cli client.Client
}

type Relation struct {
	id string
	md meta.Metadata
}
