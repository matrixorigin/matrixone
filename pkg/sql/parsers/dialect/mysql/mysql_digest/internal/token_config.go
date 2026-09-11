// Copyright 2026 Matrix Origin
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
//
// Portions derived from github.com/rashiq/mysql-digest; see ../LICENSE.

package internal

type TokenConfig struct {
	Keywords map[string]int
}

func (c *TokenConfig) LookupKeyword(word string) int {
	return c.Keywords[word]
}

func (c *TokenConfig) GetString(tok int) string {
	return TokenString(tok)
}

var configMySQL84 = buildMySQL84Config()

func GetTokenConfig() *TokenConfig {
	return configMySQL84
}

// These tokens remain reserved in MySQL 8.4, but their former spellings from
// MySQL 8.0 are identifiers rather than keywords.
var removedMySQL80Keywords = map[string]bool{
	"GET_MASTER_PUBLIC_KEY":         true,
	"MASTER_AUTO_POSITION":          true,
	"MASTER_BIND":                   true,
	"MASTER_COMPRESSION_ALGORITHMS": true,
	"MASTER_CONNECT_RETRY":          true,
	"MASTER_DELAY":                  true,
	"MASTER_HEARTBEAT_PERIOD":       true,
	"MASTER_HOST":                   true,
	"MASTER_LOG_FILE":               true,
	"MASTER_LOG_POS":                true,
	"MASTER_PASSWORD":               true,
	"MASTER_PORT":                   true,
	"MASTER_PUBLIC_KEY_PATH":        true,
	"MASTER_RETRY_COUNT":            true,
	"MASTER_SSL":                    true,
	"MASTER_SSL_CA":                 true,
	"MASTER_SSL_CAPATH":             true,
	"MASTER_SSL_CERT":               true,
	"MASTER_SSL_CIPHER":             true,
	"MASTER_SSL_CRL":                true,
	"MASTER_SSL_CRLPATH":            true,
	"MASTER_SSL_KEY":                true,
	"MASTER_SSL_VERIFY_SERVER_CERT": true,
	"MASTER_TLS_CIPHERSUITES":       true,
	"MASTER_TLS_VERSION":            true,
	"MASTER_ZSTD_COMPRESSION_LEVEL": true,
}

// These spellings have token numbers reserved for MySQL 9.x, but are not
// keywords in the MySQL 8.4 grammar used by STATEMENT_DIGEST.
var mysql90Keywords = map[string]bool{
	"ABSENT":                 true,
	"ALLOW_MISSING_FILES":    true,
	"AUTO_REFRESH":           true,
	"AUTO_REFRESH_SOURCE":    true,
	"DUALITY":                true,
	"EXTERNAL":               true,
	"EXTERNAL_FORMAT":        true,
	"FILES":                  true,
	"FILE_FORMAT":            true,
	"FILE_NAME":              true,
	"FILE_PATTERN":           true,
	"FILE_PREFIX":            true,
	"GUIDED":                 true,
	"HEADER":                 true,
	"JSON_DUALITY_OBJECT":    true,
	"LIBRARY":                true,
	"MATERIALIZED":           true,
	"PARAMETERS":             true,
	"RELATIONAL":             true,
	"SETS":                   true,
	"STRICT_LOAD":            true,
	"URI":                    true,
	"VALIDATE":               true,
	"VECTOR":                 true,
	"VERIFY_KEY_CONSTRAINTS": true,
}

func buildMySQL84Config() *TokenConfig {
	keywords := make(map[string]int, len(TokenKeywords))
	for word, token := range TokenKeywords {
		if removedMySQL80Keywords[word] || mysql90Keywords[word] {
			continue
		}
		keywords[word] = token
	}
	return &TokenConfig{Keywords: keywords}
}
