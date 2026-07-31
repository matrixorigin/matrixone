// Copyright 2026 Matrix Origin
// Source-level evidence for the old (device_id, minute) key blocker.
db.events.aggregate([
  {$match: {ts: {$gte: ISODate("REPLACE_LOW"), $lt: ISODate("REPLACE_HIGH")}}},
  {$project: {device_id: 1, site_id: 1, minute: {$dateTrunc: {date: "$ts", unit: "minute", timezone: "UTC"}}}},
  {$group: {_id: {device_id: "$device_id", minute: "$minute"}, sites: {$addToSet: "$site_id"}}},
  {$match: {$expr: {$gt: [{$size: "$sites"}, 1]}}},
  {$sort: {"_id.minute": 1, "_id.device_id": 1}}
], {allowDiskUse: true});
