// Copyright 2021 Matrix Origin
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

package main

import (
	"errors"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) FilterPacket() fz.FilterPacket {
	return func(packet []byte) []byte {

		var ip4 layers.IPv4
		var eth layers.Ethernet
		parser := gopacket.NewDecodingLayerParser(layers.LayerTypeEthernet, &eth, &ip4)

		var decoded []gopacket.LayerType
		err := parser.DecodeLayers(packet, &decoded)
		if err != nil {
			var unsupported gopacket.UnsupportedLayerType
			if errors.As(err, &unsupported) {
				// don't process unknown packet
				return packet
			}
		}
		ce(err)

		for _, layerType := range decoded {
			switch layerType {
			case layers.LayerTypeIPv4:
				pt("%+v\n", ip4)
			}
		}

		return packet
	}
}
