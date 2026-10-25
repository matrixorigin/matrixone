// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package message

import (
	"context"
	"errors"
)

// BroadcastSpillGroup reconstructs CN-local ownership after pipeline decoding.
// It is tied to this statement board, never to one short-lived remote RPC.
func (m *MessageBoard) BroadcastSpillGroup(tag int32, scopes []uint32) (*JoinSpillGroup, error) {
	m.spillMu.Lock()
	defer m.spillMu.Unlock()
	if group := m.spillGroups[tag]; group != nil {
		group.mu.Lock()
		same := len(group.expectedScopes) == len(scopes)
		for _, id := range scopes {
			_, ok := group.expectedScopes[id]
			same = same && ok
		}
		group.mu.Unlock()
		if !same {
			return nil, errors.New("inconsistent broadcast spill topology")
		}
		return group, nil
	}
	if len(scopes) == 0 {
		return nil, errors.New("missing broadcast spill topology")
	}
	group := NewJoinSpillGroup()
	group.BeginScopes(context.Background(), scopes)
	group.onTerminal = m.releaseSpillBoard
	if m.spillGroups == nil {
		m.spillGroups = make(map[int32]*JoinSpillGroup)
	}
	m.spillGroups[tag] = group
	return group, nil
}

func (m *MessageBoard) retainSpillBoardOnReset() bool {
	m.spillMu.Lock()
	defer m.spillMu.Unlock()
	m.spillResetRequested = true
	for _, group := range m.spillGroups {
		if !group.terminal() {
			return true
		}
	}
	return false
}

func (m *MessageBoard) releaseSpillBoard() {
	m.spillMu.Lock()
	ready := m.spillResetRequested
	for _, group := range m.spillGroups {
		ready = ready && group.terminal()
	}
	m.spillMu.Unlock()
	if ready && m.messageCenter != nil {
		m.messageCenter.RwMutex.Lock()
		if m.messageCenter.StmtIDToBoard[m.stmtId] == m {
			delete(m.messageCenter.StmtIDToBoard, m.stmtId)
		}
		m.messageCenter.RwMutex.Unlock()
	}
}

func (m *MessageBoard) abortSpillGroups() {
	m.spillMu.Lock()
	groups := m.spillGroups
	m.spillGroups = nil
	m.spillMu.Unlock()
	for _, group := range groups {
		group.Abort(0)
	}
}
