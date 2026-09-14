// Copyright 2021 - 2024 Matrix Origin
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

package geo

import (
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// geohashBase32 is the standard geohash alphabet (no a, i, l, o).
const geohashBase32 = "0123456789bcdefghjkmnpqrstuvwxyz"

const (
	maxGeoHashLength       = 100
	maxGeoHashDecodeLength = 433
)

// EncodeGeoHash encodes a (longitude, latitude) position into a geohash string
// of the given character length. Bits interleave longitude-first; each base32
// character carries 5 bits. Coordinates are inclusive at the geographic
// bounds, and maxLength follows the GeoHash SQL function contract.
func EncodeGeoHash(lon, lat float64, maxLength int64) (string, error) {
	if math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 {
		return "", moerr.NewInvalidInputNoCtxf("longitude must be finite and in [-180,180], got %v", lon)
	}
	if math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
		return "", moerr.NewInvalidInputNoCtxf("latitude must be finite and in [-90,90], got %v", lat)
	}
	if maxLength < 1 || maxLength > maxGeoHashLength {
		return "", moerr.NewInvalidInputNoCtxf("geohash length must be in [1,%d], got %d", maxGeoHashLength, maxLength)
	}
	length := int(maxLength)
	lonMin, lonMax := -180.0, 180.0
	latMin, latMax := -90.0, 90.0
	hash := make([]byte, 0, length)
	bits, bitCount := 0, 0
	even := true // longitude on even bits
	for len(hash) < length {
		if even {
			mid := (lonMin + lonMax) / 2
			if lon >= mid {
				bits = bits<<1 | 1
				lonMin = mid
			} else {
				bits <<= 1
				lonMax = mid
			}
		} else {
			mid := (latMin + latMax) / 2
			if lat >= mid {
				bits = bits<<1 | 1
				latMin = mid
			} else {
				bits <<= 1
				latMax = mid
			}
		}
		even = !even
		if bitCount++; bitCount == 5 {
			hash = append(hash, geohashBase32[bits])
			bits, bitCount = 0, 0
		}
	}
	return string(hash), nil
}

// DecodeGeoHash decodes a geohash into the center longitude/latitude of the cell
// it denotes.
func DecodeGeoHash(hash string) (lon, lat float64, err error) {
	if len(hash) == 0 {
		return 0, 0, moerr.NewInvalidInputNoCtx("geohash must not be empty")
	}
	lonMin, lonMax := -180.0, 180.0
	latMin, latMax := -90.0, 90.0
	even := true
	limit := len(hash)
	if limit > maxGeoHashDecodeLength {
		limit = maxGeoHashDecodeLength
	}
	for i := 0; i < limit; i++ {
		c := hash[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		idx := strings.IndexByte(geohashBase32, c)
		if idx < 0 {
			return 0, 0, moerr.NewInvalidInputNoCtxf("invalid geohash character %q", c)
		}
		for bit := 4; bit >= 0; bit-- {
			b := (idx >> bit) & 1
			if even {
				mid := (lonMin + lonMax) / 2
				if b == 1 {
					lonMin = mid
				} else {
					lonMax = mid
				}
			} else {
				mid := (latMin + latMax) / 2
				if b == 1 {
					latMin = mid
				} else {
					latMax = mid
				}
			}
			even = !even
		}
	}
	return (lonMin + lonMax) / 2, (latMin + latMax) / 2, nil
}
