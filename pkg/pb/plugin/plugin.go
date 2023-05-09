package plugin

import (
	"bytes"
	"fmt"
)

// SetID implement morpc Messgae
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Request) GetID() uint64 {
	return m.RequestID
}

// DebugString returns the debug string
func (m *Request) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.ClientInfo.String())
	buffer.WriteString("/")
	return buffer.String()
}

func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Response) GetID() uint64 {
	return m.RequestID
}

func (m *Response) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.Recommendation.String())
	buffer.WriteString("/")
	return buffer.String()
}
