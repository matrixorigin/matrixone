// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package message

import "errors"

// RetainStatementMemory keeps the board reachable while later RPC scopes of
// the same statement have not started yet. A normal individual scope Reset
// cannot retire the statement's messages or its shared spill registry.
func (m *MessageBoard) RetainStatementMemory() error {
	m.spillMu.Lock()
	defer m.spillMu.Unlock()
	if m.memoryRootClosed {
		return errors.New("statement memory owner already closed")
	}
	m.memoryRoots++
	return nil
}

func (m *MessageBoard) retainMemoryBoardOnReset() bool {
	m.spillMu.Lock()
	defer m.spillMu.Unlock()
	if m.memoryRoots == 0 {
		return false
	}
	m.spillResetRequested = true
	return true
}

// ReleaseStatementMemory is called by the independent statement control owner,
// after normal scope completion or cancellation. Message and spill-source refs
// held by a running consumer remain that consumer's responsibility.
func (m *MessageBoard) ReleaseStatementMemory() {
	m.spillMu.Lock()
	if m.memoryRoots == 0 {
		m.spillMu.Unlock()
		return
	}
	m.memoryRoots--
	final := m.memoryRoots == 0
	if final {
		m.memoryRootClosed = true
	}
	m.spillMu.Unlock()
	if !final {
		return
	}
	m.cleanupQueuedMessages()
	if m.messageCenter != nil {
		m.messageCenter.RwMutex.Lock()
		if m.messageCenter.StmtIDToBoard[m.stmtId] == m {
			delete(m.messageCenter.StmtIDToBoard, m.stmtId)
		}
		m.messageCenter.RwMutex.Unlock()
	}
}
