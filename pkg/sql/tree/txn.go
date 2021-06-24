package tree

//the read and write mode for a transaction.
type ReadWriteMode int

const (
	READ_WRITE_MODE_NONE 		ReadWriteMode = iota
	READ_WRITE_MODE_READ_ONLY
	READ_WRITE_MODE_READ_WRITE
)

//modes for a transaction
type TransactionModes struct {
	rwMode ReadWriteMode
}

func MakeTransactionModes(rwm ReadWriteMode) TransactionModes {
	return TransactionModes{rwMode: rwm}
}

//Begin statement
type BeginTransaction struct {
	statementImpl
	Modes TransactionModes
}

func NewBeginTransaction(m TransactionModes) *BeginTransaction {
	return &BeginTransaction{Modes: m}
}

type CompletionType int

const (
	COMPLETION_TYPE_NO_CHAIN CompletionType = iota
	COMPLETION_TYPE_CHAIN
	COMPLETION_TYPE_RELEASE
)

//Commit statement
type CommitTransaction struct {
	statementImpl
	Type CompletionType
}

func NewCommitTransaction(t CompletionType) *CommitTransaction {
	return &CommitTransaction{Type: t}
}

//Rollback statement
type RollbackTransaction struct{
	statementImpl
	Type CompletionType
}

func NewRollbackTransaction(t CompletionType) *RollbackTransaction {
	return &RollbackTransaction{Type: t}
}