package tables

type Operation interface {
	OpName() string
}

type forceCompactOp struct{}

func (op *forceCompactOp) OpName() string { return "ForceCompactOp" }

type unloadOp struct{}

func (op *unloadOp) OpName() string { return "UnloadOp" }

type loadOp struct{}

func (op *loadOp) OpName() string { return "LoadOp" }
