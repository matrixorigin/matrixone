// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	Endian = binary.LittleEndian
)

// Version is used to control proxy and cn version to keep them compatibility.
type Version struct {
	Major int32
	Minor int32
	Patch int32
}

func (v *Version) GE(v1 Version) bool {
	if v.Major < v1.Major {
		return false
	}
	if v.Minor < v1.Minor {
		return false
	}
	if v.Patch < v1.Patch {
		return false
	}
	return true
}

func (v *Version) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, Endian, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (v *Version) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, Endian, v); err != nil {
		return err
	}
	return nil
}

var (
	// Version0 is the first version, whose major and minor are both -1.
	Version0 = Version{Major: 1, Minor: 0, Patch: 0}

	availableVersions = map[Version]struct{}{
		Version0: {},
	}
)

func (v *Version) available() bool {
	if _, ok := availableVersions[*v]; !ok {
		return false
	}
	return true
}

func (v *Version) compatible(r Version) bool {
	// The version of local ExtraInfo should be greater than or equal to
	// the remote one.
	return v.GE(r)
}

// ExtraInfoV0 is the information that proxy send to CN when build
// connection in handshake phase.
type ExtraInfoV0 struct {
	// Salt is the bytes in MySQL protocol which is 20 length.
	Salt []byte
	// InternalConn indicates whether the connection is from internal
	// network or external network. false means that it is external, otherwise,
	// it is internal.
	InternalConn bool
	// ConnectionID sent from proxy, cn side should update its connection ID.
	ConnectionID uint32
	// Label is the requested label from client.
	Label RequestLabel
}

// GetSalt implements the InfoFetcher interface.
func (i *ExtraInfoV0) GetSalt() ([]byte, bool) {
	return i.Salt, true
}

// GetInternalConn implements the InfoFetcher interface.
func (i *ExtraInfoV0) GetInternalConn() (bool, bool) {
	return i.InternalConn, true
}

// GetConnectionID implements the InfoFetcher interface.
func (i *ExtraInfoV0) GetConnectionID() (uint32, bool) {
	return i.ConnectionID, true
}

// GetLabel implements the InfoFetcher interface.
func (i *ExtraInfoV0) GetLabel() (RequestLabel, bool) {
	return i.Label, true
}

// Marshal implements the Codec interface.
func (i *ExtraInfoV0) Marshal(v Version) ([]byte, error) {
	if len(i.Salt) != 20 {
		return nil, moerr.NewInternalErrorNoCtx("invalid slat data, length is %d", len(i.Salt))
	}
	var err error
	var labelData []byte
	labelData, err = i.Label.Marshal()
	if err != nil {
		return nil, err
	}
	vData, err := v.Encode()
	if err != nil {
		return nil, err
	}
	size := uint16(len(labelData)+len(vData)) + 20 + 1 + 4
	buf := new(bytes.Buffer)
	if err = binary.Write(buf, Endian, size); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, Endian, vData); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, Endian, i.Salt); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, Endian, i.InternalConn); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, Endian, i.ConnectionID); err != nil {
		return nil, err
	}
	if len(labelData) > 0 {
		err = binary.Write(buf, Endian, labelData)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// Unmarshal implements the Codec interface.
func (i *ExtraInfoV0) Unmarshal(data []byte) error {
	if len(data) < 25 {
		return moerr.NewInternalErrorNoCtx("invalid data length %d", len(data))
	}
	buf := bytes.NewBuffer(data)
	i.Salt = make([]byte, 20)
	if err := binary.Read(buf, Endian, i.Salt); err != nil {
		return err
	}
	if err := binary.Read(buf, Endian, &i.InternalConn); err != nil {
		return err
	}
	if err := binary.Read(buf, Endian, &i.ConnectionID); err != nil {
		return err
	}
	if len(data) > 25 {
		labelData := make([]byte, len(data)-25)
		if err := binary.Read(buf, Endian, labelData); err != nil {
			return err
		}
		if err := i.Label.Unmarshal(labelData); err != nil {
			return err
		}
	}
	return nil
}

type Codec interface {
	// Marshal encodes the extra info and the version then returns bytes.
	Marshal(version Version) ([]byte, error)
	// Unmarshal decodes information from the bytes.
	Unmarshal([]byte) error
}

// InfoFetcher is the interface used to fetch information.
// The last return value of all methods in the interface must be
// a boolean value to indicates whether the method is supported
// by the version.
type InfoFetcher interface {
	// GetSalt returns the salt value.
	GetSalt() ([]byte, bool)
	// GetInternalConn returns the internal conn value.
	GetInternalConn() (bool, bool)
	// GetConnectionID returns the connection ID value.
	GetConnectionID() (uint32, bool)
	// GetLabel returns the label value.
	GetLabel() (RequestLabel, bool)
}

// ExtraInfo is the extra information between proxy and cn.
type ExtraInfo interface {
	Codec
	InfoFetcher
}

type VersionedExtraInfo struct {
	// Version defines the version of extra info.
	Version   Version
	ExtraInfo ExtraInfo
}

// NewVersionedExtraInfo creates a new ExtraInfo instance with specified version.
func NewVersionedExtraInfo(v Version, i ExtraInfo) *VersionedExtraInfo {
	return &VersionedExtraInfo{Version: v, ExtraInfo: i}
}

func (e *VersionedExtraInfo) Encode() ([]byte, error) {
	if !e.Version.available() {
		return nil, moerr.NewInternalErrorNoCtx("invalid version %+v", e.Version)
	}
	return e.ExtraInfo.Marshal(e.Version)
}

// readData reads all data in the reader.
func readData(reader *bufio.Reader) ([]byte, error) {
	s, err := reader.Peek(2)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(s)
	var size uint16
	err = binary.Read(buf, Endian, &size)
	if err != nil {
		return nil, err
	}
	size += 2
	data := make([]byte, size)
	if reader.Buffered() < int(size) {
		hr := 0
		for hr < int(size) {
			l, err := reader.Read(data[hr:])
			if err != nil {
				return nil, err
			}
			hr += l
		}
	} else {
		_, err = reader.Read(data)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (e *VersionedExtraInfo) createExtraInfo(v Version) error {
	switch v {
	case Version0:
		e.ExtraInfo = &ExtraInfoV0{}
	default:
		return moerr.NewInternalErrorNoCtx("invalid version: %+v", e.Version)
	}
	return nil
}

func (e *VersionedExtraInfo) Decode(reader *bufio.Reader) error {
	data, err := readData(reader)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data[2:])
	var v Version
	if err = binary.Read(buf, binary.LittleEndian, &v); err != nil {
		return err
	}
	if !e.Version.compatible(v) {
		return moerr.NewInternalErrorNoCtx("version incompatible: local: %v, peer: %v",
			e.Version, v)
	}
	if err := e.createExtraInfo(v); err != nil {
		return err
	}
	return e.ExtraInfo.Unmarshal(buf.Bytes())
}
