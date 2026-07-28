// Copyright 2026 Matrix Origin
// Source-level evidence for the old (pump, minute) key blocker.
db.raw.aggregate([
  {$match: {ts: {$gte: ISODate("REPLACE_LOW"), $lt: ISODate("REPLACE_HIGH")}}},
  {$project: {pump: 1, crew: 1, minute: {$dateTrunc: {date: "$ts", unit: "minute", timezone: "UTC"}}}},
  {$group: {_id: {pump: "$pump", minute: "$minute"}, crews: {$addToSet: "$crew"}}},
  {$match: {$expr: {$gt: [{$size: "$crews"}, 1]}}},
  {$sort: {"_id.minute": 1, "_id.pump": 1}}
], {allowDiskUse: true});
