// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

const source = db.getSiblingDB("mongodb_source");
if (!source.getUser("mo_reader")) {
  source.createUser({
    user: "mo_reader",
    pwd: process.env.MONGODB_READER_PASSWORD,
    roles: [{role: "read", db: "mongodb_source"}]
  });
}
if (!source.getUser("mo_reader_next")) {
  source.createUser({
    user: "mo_reader_next",
    pwd: process.env.MONGODB_READER_NEXT_PASSWORD,
    roles: [{role: "read", db: "mongodb_source"}]
  });
}
source.events.drop();
source.events.insertMany([
  {_id: ObjectId("64b000000000000000000001"), device_id: "device-001", site_id: "site-east", ts: ISODate("2026-07-27T10:00:05Z"), measurement: 10.0, source_batch: "batch-001"},
  {_id: ObjectId("64b000000000000000000002"), device_id: "device-001", site_id: "site-east", ts: ISODate("2026-07-27T10:00:35Z"), measurement: 14.0, source_batch: null},
  {_id: ObjectId("64b000000000000000000003"), device_id: "device-001", site_id: "site-east", ts: ISODate("2026-07-27T10:02:05Z"), measurement: 20.0},
  {_id: ObjectId("64b000000000000000000004"), device_id: "device-001", site_id: "site-west", ts: ISODate("2026-07-27T10:00:15Z"), measurement: 30.0, source_batch: "batch-002"},
  {_id: ObjectId("64b000000000000000000005"), device_id: "device-002", site_id: "site-east", ts: ISODate("2026-07-27T10:01:00Z"), measurement: "malformed"}
]);
source.events.createIndex({ts: 1, _id: 1});

source.temporal_edges.drop();
source.temporal_edges.insertOne({
  _id: ObjectId("64b000000000000000000101"),
  ts: ISODate("2026-07-27T10:00:05.100Z")
});
source.temporal_edges.createIndex({ts: 1});

source.decoded_budget.drop();
source.decoded_budget.insertOne({
  _id: ObjectId("64b000000000000000000201"),
  payload: "x".repeat(192 * 1024)
});

source.json_scalar.drop();
source.json_scalar.insertOne({
  _id: 1,
  value: "text",
  payload: {a: NumberInt(2)},
  arr: [NumberInt(1), NumberInt(2)]
});

source.binary_padding.drop();
source.binary_padding.insertMany([
  {_id: "d1", value: BinData(0, "YQ==")},
  {_id: "d2", value: BinData(0, "YSA=")},
  {_id: "d3", value: BinData(0, "YSAg")},
  {_id: "d4", value: BinData(0, "QQ==")}
]);

source.temporal_order.drop();
source.temporal_order.insertMany([
  {_id: "a", d: ISODate("2026-03-08T01:59:59.123Z"), t: ISODate("2026-03-08T01:59:59.123Z")},
  {_id: "b", d: ISODate("2026-03-08T03:00:00.456Z"), t: ISODate("2026-03-08T03:00:00.456Z")},
  {_id: "c", d: ISODate("2026-11-01T01:59:59.789Z"), t: ISODate("2026-11-01T01:59:59.789Z")},
  {_id: "d", d: ISODate("2026-11-01T02:00:00.012Z"), t: ISODate("2026-11-01T02:00:00.012Z")}
]);
