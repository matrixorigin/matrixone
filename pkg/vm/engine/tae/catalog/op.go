package catalog

type OpT int8

const (
	OpCreate OpT = iota
	OpUpdate
	OpSoftDelete
	OpHardDelete
)

var OpNames = map[OpT]string{
	OpCreate:     "Create",
	OpUpdate:     "Update",
	OpSoftDelete: "SoftDelete",
	OpHardDelete: "HardDelete",
}

func OpName(op OpT) string {
	return OpNames[op]
}
