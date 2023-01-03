// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package morpc

import (
	"context"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
)

// Message morpc is not a normal remote method call, rather it is a message-based asynchronous
// driven framework.
type Message interface {
	// SetID each message has a unique ID in a RPCClient Backend. If it is a message transmitted
	// in stream, the ID must be set to Stream.ID.
	SetID(uint64)
	// GetID returns ID of the message
	GetID() uint64
	// DebugString return debug string
	DebugString() string
	// Size size of message after marshal
	Size() int
	// MarshalTo marshal to target byte slice
	MarshalTo(data []byte) (int, error)
	// Unmarshal unmarshal from data
	Unmarshal(data []byte) error
}

// PayloadMessage is similar message, but has a large payload field. To avoid redundant copy of
// memory, the encoding is msgTotalSize(4 bytes) + flag(1 byte) + messageWithoutPayloadSize(4 bytes)
// + messageWithoutPayload + payload, all fields except payload will be written to the buffer of
// each link before being written to the socket. payload, being a []byte, can be written directly
// to the socket to avoid a copy from the buffer to the socket.
//
// Note: When decoding, all the socket data will be read into the buffer, the payload data will not
// be copied from the buffer once, but directly using the slice of the buffer data to call SetPayloadField.
// so this is not safe and needs to be used very carefully, i.e. after processing the message back to the rpc
// framework, this data cannot be held.
type PayloadMessage interface {
	Message

	// GetPayloadField return the payload data
	GetPayloadField() []byte
	// SetPayloadField set the payload data
	SetPayloadField(data []byte)
}

// RPCMessage any message sent via morpc needs to have a Context set, which is transmitted across the network.
// So messages sent and received at the network level are RPCMessage.
type RPCMessage struct {
	// Ctx context
	Ctx context.Context
	// Message raw rpc message
	Message Message

	cancel         context.CancelFunc
	stream         bool
	streamSequence uint32
}

// RPCClient morpc is not a normal remote method call, rather it is a message-based asynchronous
// driven framework. Each message has a unique ID, and the response to this message must have the
// same ID.
type RPCClient interface {
	// Send send a request message to the corresponding server and return a Future to get the
	// response message.
	Send(ctx context.Context, backend string, request Message) (*Future, error)
	// NewStream create a stream used to asynchronous stream of sending and receiving messages.
	// If the underlying connection is reset during the duration of the stream, then the stream will
	// be closed.
	NewStream(backend string, lock bool) (Stream, error)
	// Close close the client
	Close() error
}

// ClientSession client session, which is used to send the response message.
// Note that it is not thread-safe.
type ClientSession interface {
	// Close close the client session
	Close() error
	// Write writing the response message to the client.
	Write(ctx context.Context, response Message) error
}

// RPCServer RPC server implementation corresponding to RPCClient.
type RPCServer interface {
	// Start start listening and wait for client messages. After each client link is established,
	// a separate goroutine is assigned to handle the Read, and the Read-to message is handed over
	// to the Handler for processing.
	Start() error
	// Close close the rpc server
	Close() error
	// RegisterRequestHandler register the request handler. The request handler is processed in the
	// read goroutine of the current client connection. Sequence is the sequence of message received
	// by the current client connection. If error returned by handler, client connection will closed.
	// Handler can use the ClientSession to write response, both synchronous and asynchronous.
	RegisterRequestHandler(func(ctx context.Context, request Message, sequence uint64, cs ClientSession) error)
}

// Codec codec
type Codec interface {
	codec.Codec
	// AddHeaderCodec add header codec. The HeaderCodecs are added sequentially and the headercodecs are
	// executed in the order in which they are added at codec time.
	AddHeaderCodec(HeaderCodec)
}

// HeaderCodec encode and decode header
type HeaderCodec interface {
	// Encode encode header into output buffer
	Encode(*RPCMessage, *buf.ByteBuf) (int, error)
	// Decode decode header from input buffer
	Decode(*RPCMessage, []byte) (int, error)
}

// BackendFactory backend factory
type BackendFactory interface {
	// Create create the corresponding backend based on the given address.
	Create(address string) (Backend, error)
}

// Backend backend represents a wrapper for a client communicating with a
// remote server.
type Backend interface {
	// Send send the request for future to the corresponding backend.
	// moerr.ErrBackendClosed returned if backend is closed.
	Send(ctx context.Context, request Message) (*Future, error)
	// NewStream create a stream used to asynchronous stream of sending and receiving messages.
	// If the underlying connection is reset during the duration of the stream, then the stream will
	// be closed.
	NewStream(unlockAfterClose bool) (Stream, error)
	// Close close the backend.
	Close()
	// Busy the backend receives a lot of requests concurrently during operation, but when the number
	// of requests waiting to be sent reaches some threshold, the current backend is busy.
	Busy() bool
	// LastActiveTime returns last active time
	LastActiveTime() time.Time
	// Lock other I/O operations can not use this backend if the backend is locked
	Lock()
	// Unlock the backend can used by other I/O operations after unlock
	Unlock()
	// Locked indicates if backend is locked
	Locked() bool
}

// Stream used to asynchronous stream of sending and receiving messages
type Stream interface {
	// ID returns the stream ID. All messages transmitted on the current stream need to use the
	// stream ID as the message ID
	ID() uint64
	// Send send message to stream
	Send(ctx context.Context, request Message) error
	// Receive returns a channel to read stream message from server. If nil is received, the receive
	// loop needs to exit. In any case, Stream.Close needs to be called.
	Receive() (chan Message, error)
	// Close close the stream.
	Close() error
}

// ClientOption client options for create client
type ClientOption func(*client)

// ServerOption server options for create rpc server
type ServerOption func(*server)

// BackendOption options for create remote backend
type BackendOption func(*remoteBackend)

// CodecOption codec options
type CodecOption func(*messageCodec)
