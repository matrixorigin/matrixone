package logEngine

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/logEngine/meta/client"
	"matrixone/pkg/vm/metadata"
)

func New(cli client.Client) *LogEngine {
	return &LogEngine{cli}
}

func (e *LogEngine) Delete(name string) error {
	return nil
}

func (e *LogEngine) Create(name string, attrs []metadata.Attribute) error {
	return nil
}

func (e *LogEngine) Relations() []engine.Relation {
	return nil
}

func (e *LogEngine) Relation(name string) (engine.Relation, error) {
	return nil, nil
}
