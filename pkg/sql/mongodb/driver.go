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

package mongodb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// FindSpec is deliberately driver-neutral at the operator boundary.
type FindSpec struct {
	Filter     any
	Projection any
	Sort       any
	BatchSize  int32
	Limit      int64
}

type Cursor interface {
	Next(context.Context) bool
	CurrentRaw() []byte
	Err() error
	Close(context.Context) error
}

type Collection interface {
	Find(context.Context, FindSpec) (Cursor, error)
}

type Client interface {
	Collection(database, collection string) Collection
	Ping(context.Context) error
	Disconnect(context.Context) error
}

type ClientFactory interface {
	Connect(context.Context, Connection, Credentials, RuntimeConfig) (Client, error)
}

type OfficialClientFactory struct{}

type connectorOptions struct {
	Direct bool `json:"direct"`
}

func decodeConnectorOptions(ctx context.Context, raw string) (connectorOptions, error) {
	var decoded connectorOptions
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return decoded, nil
	}
	if !strings.HasPrefix(raw, "{") {
		return decoded, moerr.NewInvalidInput(ctx, "MongoDB options_json must be a JSON object")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return connectorOptions{}, moerr.NewInvalidInput(ctx, "MongoDB options_json is invalid or contains an unsupported option")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return connectorOptions{}, moerr.NewInvalidInput(ctx, "MongoDB options_json must contain exactly one JSON object")
	}
	return decoded, nil
}

func (OfficialClientFactory) Connect(ctx context.Context, connection Connection, credentials Credentials, cfg RuntimeConfig) (Client, error) {
	if err := ValidateConnection(ctx, connection, cfg); err != nil {
		return nil, err
	}
	clientOptions, err := buildClientOptions(ctx, connection, credentials, cfg)
	if err != nil {
		return nil, err
	}
	if err := ValidateResolvedEndpoints(ctx, connection, cfg, nil); err != nil {
		return nil, err
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, "MongoDB client configuration failed")
	}
	wrapper := &officialClient{client: client, readPreference: clientOptions.ReadPreference}
	if err := wrapper.Ping(ctx); err != nil {
		_ = disconnectClients([]Client{wrapper})
		return nil, moerr.NewInternalError(ctx, "MongoDB server selection or authentication failed")
	}
	return wrapper, nil
}

func buildClientOptions(ctx context.Context, connection Connection, credentials Credentials, cfg RuntimeConfig) (*options.ClientOptions, error) {
	if credentials.Username == "" || credentials.Password == "" {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB credential secret resolved to an incomplete identity")
	}
	if connection.TLSCASecretRef != "" && len(credentials.TLSCA) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB TLS CA secret resolved to empty material")
	}
	commandMonitor, poolMonitor, serverMonitor := newDriverMonitors()
	clientOptions := options.Client().
		SetAppName("MatrixOne MongoScan").
		SetMonitor(commandMonitor).
		SetPoolMonitor(poolMonitor).
		SetServerMonitor(serverMonitor).
		SetDialer(newPolicyDialer(cfg, nil)).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetTimeout(cfg.SocketTimeout).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxConnecting(cfg.MaxConnecting).
		// The initial find remains a retryable read, but an in-progress
		// cursor must never hide a getMore failure behind an adaptive retry.
		// MongoScan fails the statement and replays from the committed
		// watermark instead of risking an ambiguous partial cursor.
		SetMaxAdaptiveRetries(0).
		SetRetryReads(true).
		SetRetryWrites(false)
	connectorOptions, err := decodeConnectorOptions(ctx, connection.OptionsJSON)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(connection.OptionsJSON) != "" {
		clientOptions.SetDirect(connectorOptions.Direct)
	}

	if strings.TrimSpace(connection.SRVHost) != "" {
		clientOptions.ApplyURI("mongodb+srv://" + strings.TrimSpace(connection.SRVHost))
	} else {
		clientOptions.SetHosts(splitSeeds(connection.Hosts))
		if connection.ReplicaSet != "" {
			clientOptions.SetReplicaSet(connection.ReplicaSet)
		}
	}
	if credentials.Username != "" {
		clientOptions.SetAuth(options.Credential{
			AuthMechanism: connection.AuthMechanism,
			AuthSource:    defaultString(connection.AuthSource, "admin"),
			Username:      credentials.Username,
			Password:      credentials.Password,
			PasswordSet:   true,
		})
	}

	rp, err := parseReadPreference(connection.ReadPreference, connection.MaxStalenessSeconds)
	if err != nil {
		return nil, moerr.NewInvalidInput(ctx, "unsupported MongoDB read preference")
	}
	clientOptions.SetReadPreference(rp)
	switch strings.ToLower(connection.ReadConcern) {
	case "", "majority":
		clientOptions.SetReadConcern(readconcern.Majority())
	case "local":
		clientOptions.SetReadConcern(readconcern.Local())
	default:
		return nil, moerr.NewInvalidInput(ctx, "unsupported MongoDB read concern")
	}

	tlsMode := strings.ToLower(strings.TrimSpace(connection.TLSMode))
	if err := configureTLS(ctx, clientOptions, tlsMode, credentials.TLSCA); err != nil {
		return nil, err
	}
	if err := clientOptions.Validate(); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "invalid MongoDB client configuration")
	}
	return clientOptions, nil
}

func configureTLS(ctx context.Context, clientOptions *options.ClientOptions, tlsMode string, caPEM []byte) error {
	if tlsMode == "disabled" {
		// mongodb+srv enables TLS implicitly. The catalog surface makes the TLS
		// mode explicit, so override that URI default instead of silently
		// connecting with behavior different from SHOW CREATE.
		clientOptions.SetTLSConfig(nil)
	} else if tlsMode != "" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if len(caPEM) > 0 {
			roots, err := x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(caPEM) {
				return moerr.NewInvalidInput(ctx, "MongoDB TLS CA secret does not contain a valid certificate")
			}
			tlsConfig.RootCAs = roots
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}
	return nil
}

func splitSeeds(hosts string) []string {
	parts := strings.Split(hosts, ",")
	seeds := make([]string, 0, len(parts))
	for _, host := range parts {
		if host = strings.TrimSpace(host); host != "" {
			seeds = append(seeds, host)
		}
	}
	return seeds
}

func parseReadPreference(value string, maxStalenessSeconds int64) (*readpref.ReadPref, error) {
	modeValue := strings.ToLower(strings.TrimSpace(value))
	if modeValue == "" {
		modeValue = "secondarypreferred"
	}
	mode, err := readpref.ModeFromString(modeValue)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtx("unsupported MongoDB read preference")
	}
	options := make([]readpref.Option, 0, 1)
	if maxStalenessSeconds > 0 {
		options = append(options, readpref.WithMaxStaleness(time.Duration(maxStalenessSeconds)*time.Second))
	}
	return readpref.New(mode, options...)
}

type officialClient struct {
	client         *mongo.Client
	readPreference *readpref.ReadPref
}

func (c *officialClient) Collection(database, collection string) Collection {
	return officialCollection{collection: c.client.Database(database).Collection(collection)}
}

func (c *officialClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, c.readPreference)
}

func (c *officialClient) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

type officialCollection struct {
	collection *mongo.Collection
}

func (c officialCollection) Find(ctx context.Context, spec FindSpec) (Cursor, error) {
	filter := spec.Filter
	if filter == nil {
		filter = bson.D{}
	}
	findOptions := options.Find()
	if spec.Projection != nil {
		findOptions.SetProjection(spec.Projection)
	}
	if spec.Sort != nil {
		findOptions.SetSort(spec.Sort)
	}
	if spec.BatchSize > 0 {
		findOptions.SetBatchSize(spec.BatchSize)
	}
	if spec.Limit > 0 {
		findOptions.SetLimit(spec.Limit)
	}
	// Driver v2 intentionally removed the cursor-operation maxTimeMS option.
	// The client operation timeout and this statement context bound both the
	// initial find and every getMore without creating a second timeout source.
	cursor, err := c.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	return &officialCursor{cursor: cursor}, nil
}

type officialCursor struct {
	cursor *mongo.Cursor
}

func (c *officialCursor) Next(ctx context.Context) bool   { return c.cursor.Next(ctx) }
func (c *officialCursor) CurrentRaw() []byte              { return c.cursor.Current }
func (c *officialCursor) Err() error                      { return c.cursor.Err() }
func (c *officialCursor) Close(ctx context.Context) error { return c.cursor.Close(ctx) }
