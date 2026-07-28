// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

const source = db.getSiblingDB("nesr_source");
if (!source.getUser("mo_reader")) {
  source.createUser({
    user: "mo_reader",
    pwd: process.env.MONGODB_READER_PASSWORD,
    roles: [{role: "read", db: "nesr_source"}]
  });
}
if (!source.getUser("mo_reader_next")) {
  source.createUser({
    user: "mo_reader_next",
    pwd: process.env.MONGODB_READER_NEXT_PASSWORD,
    roles: [{role: "read", db: "nesr_source"}]
  });
}
source.raw.drop();
source.raw.insertMany([
  {_id: ObjectId("64b000000000000000000001"), pump: "p1", crew: "a", ts: ISODate("2026-07-27T10:00:05Z"), value: 10.0, batch: "b1"},
  {_id: ObjectId("64b000000000000000000002"), pump: "p1", crew: "a", ts: ISODate("2026-07-27T10:00:35Z"), value: 14.0, batch: null},
  {_id: ObjectId("64b000000000000000000003"), pump: "p1", crew: "a", ts: ISODate("2026-07-27T10:02:05Z"), value: 20.0},
  {_id: ObjectId("64b000000000000000000004"), pump: "p1", crew: "b", ts: ISODate("2026-07-27T10:00:15Z"), value: 30.0, batch: "b2"},
  {_id: ObjectId("64b000000000000000000005"), pump: "p2", crew: "a", ts: ISODate("2026-07-27T10:01:00Z"), value: "malformed"}
]);
source.raw.createIndex({ts: 1, _id: 1});
