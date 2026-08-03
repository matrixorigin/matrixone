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
