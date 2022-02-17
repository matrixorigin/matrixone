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

package orderedcodec

const (
	//encoding definitions
	nullEncoding = 0

	//marker
	notNullEncoding = 1

	maxLengthOfInetegerEncoding = 8

	encodingPrefixForBytes byte = 18

	encodingPrefixForBytesDesc = encodingPrefixForBytes + 1

	//for integer minimum
	encodingPrefixForIntegerMinimum = 128

	//136
	encodingPrefixForIntegerZero = encodingPrefixForIntegerMinimum + maxLengthOfInetegerEncoding

	//109
	encodingPrefixForSplit = encodingPrefixForIntMax - encodingPrefixForIntegerZero - maxLengthOfInetegerEncoding

	//for integer maximum
	encodingPrefixForIntMax = 253

	//not null descending
	notNullEncodingForDesc = 254

	//null descending
	nullEncodingForDesc = 255
)