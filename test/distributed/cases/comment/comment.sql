-- test create table
drop table if exists t1;
create table t1(
col1 int comment '这是第一列',
col2 float comment '"%$^&*()_+@!',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlfU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst4gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMWh71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
);

show create table t1;
drop table t1;

drop table if exists t2;
-- echo Comment for field 'col3' is too long (max = 1024)
create table t2(
col1 int comment '这是第一列',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlfU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst4gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
);

show create table t2;
drop table t2;

drop table if exists t3;
create table t3(
col1 int comment '"这是第一列"/',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst4gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
) comment '这是一个t3表';

show create table t3;
drop table t3;


drop table if exists t4;
create table t4(
col1 int comment '"这是第一列"/',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst4gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
) comment '这是一个t4表';


show create table t4;
drop table t4;


drop table if exists t5;
create table t5(
col1 int comment '"这是第一列"/',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst5gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
) comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst5gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM';


show create table t5;
drop table t5;

drop table if exists t6;
create table t6(
col1 int comment '"这是第一列"/',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst6gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t683PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
) comment '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';


show create table t6;
drop table t6;


drop table if exists t7;
-- Comment for table 't7' is too long (max = 2048)
create table t7(
col1 int comment '"这是第一列"/',
col2 float comment '"%$^&*()_+@!"',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst6gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMW3h71ZEc70t683PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt1OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
) comment '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';


show create table t7;
drop table t7;


drop table if exists t8;
create table t8(
col1 int comment '这是第一列',
col2 float comment '"%$^&*()_+@!\'',
col3 varchar comment 'KNcR5P2ksILgsZD5lTndyuEzw49gxR2RlfU7nkNhAFOKIhig6roVYgS6yQDBkuzH790peOVKgTKUasKxuepzKqsYqQg3gDtgn0KEkC1TGVh2RU6QcdQolDbnwXsnst4gVCsF1RPp975efCff8gtXKgUtRVPdSM41vtgvKkChcUIaHU9UuLvoy6BhSm9g60VKd8NTxWRiYlzdhGiTTwtqVOq7wE9NHZt8Xq55Tz9PogoGGsgObH5llIcRAQkZUraZcYBwRoHHISouTm5whECuV8X84I0s8gx1DrQulbNCQuPVUsaAFFrsawonlvLKAOYgFPg1CheDMg63wwvY7sg1W8uCNu0ZzwmRlltC8BK1y5L1E690OV84bqNbFlkInxgl9W9CsgbIwKrFXoShkfB6DnBN5khLhH4oafYkTMWh71ZEc70t583PAxZFGNEGCALP482teY4z4Vc18EkKnG5lRg4WPNVgR5lkpFkVxvwtD2GgGvVmwdwgcYG5OlSSQGhOLDDM9sIqOlN5eyIG1kcZB8tmpMicgg7IbaKxW0ACt8OlQiLufPCvsSXlU3STSBfr3HPcbZyIGfMqkxFvpGoCHDB1D0fPlLNWIksGPOXGa6ZuXrpgdqNhFgbWTUgSllMnm59RwTZgazXXLitNVgLYK9zlVv8k6T6N0orPot2V7BvLLvxNzEvfTytliQAy418XHMb3fyR5ko34lia7hZXEqsOuEq0iTgIyHBvYn1iD3wlcnu29UTB1267O8dgL03nMmWHPFqEudVMlxeEoabRSGm2LxIlRYN8peOFBvper4Iomg7qAEaodHU1SctIGuGVTuKK5K6d2rfWs8tEokxbolTG4gxMcVzSgrcvv01eNEfCWgEYXdNShX56Wqods5qgRXNn0EeMTHyBP4tZsr6LGNgqKibYemO58VL4SE5GnGURSGW0AmFg27m1zy9qucbgAyGgDmTYGRBkxDIgSNUbVyRNe1u8RYXpWaaSGg4YwEzcgXzUKSBNS0KPXMI8GzbVM',
col4 bool comment ''
);

show create table t8;
drop table t8;

create database if not exists emis_issue;
use emis_issue;
drop table if exists `t_iot_equipment_inspection_stand`;
CREATE TABLE `t_iot_equipment_inspection_stand` (
  `id` bigint NOT NULL,
  `tenant_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '租户ID',
  `equipment_category_id` bigint NOT NULL COMMENT '设备分类ID',
  `equipment_model_id` bigint NOT NULL,
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '点检项名称',
  `category` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '检验分类：定性、定量、智能点检',
  `inspection_content` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检内容',
  `inspection_tool` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检工具',
  `inspection_method` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检方法',
  `judgment_criteria` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '判断标准',
  `inspection_exceptions` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '异常描述[\\"表面有磨损\\",\\"表面有裂痕\\"]',
  `major` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '点检专业',
  `cycle_unit` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '执行周期,一次、年、季度、月、周、日、班次、小时',
  `cycle_num` decimal(20,6) DEFAULT NULL COMMENT '周期间隔',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '备注',
  `create_org` bigint DEFAULT NULL COMMENT '创建组织',
  `use_org` bigint DEFAULT NULL COMMENT '使用组织',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `create_user` bigint DEFAULT NULL COMMENT '创建人员',
  `create_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_dept` bigint DEFAULT NULL COMMENT '创建部门',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `update_user` bigint DEFAULT NULL COMMENT '更新人员',
  `update_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `status` int DEFAULT NULL,
  `is_deleted` int DEFAULT NULL COMMENT '是否已删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC COMMENT='设备点检标准表';
show create table `t_iot_equipment_inspection_stand`;
drop database emis_issue;

create database if not exists emis_issue;
use emis_issue;
drop table if exists `t_iot_equipment_inspection_stand`;
CREATE TABLE `t_iot_equipment_inspection_stand` (
  `id` bigint NOT NULL,
  `tenant_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '租户ID',
  `equipment_category_id` bigint NOT NULL COMMENT '设备分类ID',
  `equipment_model_id` bigint NOT NULL,
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '点检项名称',
  `category` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '检验分类：定性、定量、智能点检',
  `inspection_content` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检内容',
  `inspection_tool` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检工具',
  `inspection_method` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检方法',
  `judgment_criteria` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '判断标准',
  `inspection_exceptions` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '异常描述["表面有磨损","表面有裂痕"]',
  `major` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '点检专业',
  `cycle_unit` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '执行周期,一次、年、季度、月、周、日、班次、小时',
  `cycle_num` decimal(20,6) DEFAULT NULL COMMENT '周期间隔',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '备注',
  `create_org` bigint DEFAULT NULL COMMENT '创建组织',
  `use_org` bigint DEFAULT NULL COMMENT '使用组织',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `create_user` bigint DEFAULT NULL COMMENT '创建人员',
  `create_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_dept` bigint DEFAULT NULL COMMENT '创建部门',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `update_user` bigint DEFAULT NULL COMMENT '更新人员',
  `update_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `status` int DEFAULT NULL,
  `is_deleted` int DEFAULT NULL COMMENT '是否已删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC COMMENT='设备点检标准表';
show create table `t_iot_equipment_inspection_stand`;
drop database emis_issue;

create database if not exists emis_issue;
use emis_issue;
drop table if exists `t_iot_equipment_inspection_stand`;
CREATE TABLE `t_iot_equipment_inspection_stand` (
  `id` bigint NOT NULL,
  `tenant_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '租户ID',
  `equipment_category_id` bigint NOT NULL COMMENT '设备分类ID',
  `equipment_model_id` bigint NOT NULL,
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '点检项名称',
  `category` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '检验分类：定性、定量、智能点检',
  `inspection_content` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检内容',
  `inspection_tool` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检工具',
  `inspection_method` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检方法',
  `judgment_criteria` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '判断标准',
  `inspection_exceptions` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '异常描述[**"表面有磨损",**"表面有裂痕"]',
  `major` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '点检专业',
  `cycle_unit` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '执行周期,一次、年、季度、月、周、日、班次、小时',
  `cycle_num` decimal(20,6) DEFAULT NULL COMMENT '周期间隔',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '备注',
  `create_org` bigint DEFAULT NULL COMMENT '创建组织',
  `use_org` bigint DEFAULT NULL COMMENT '使用组织',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `create_user` bigint DEFAULT NULL COMMENT '创建人员',
  `create_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_dept` bigint DEFAULT NULL COMMENT '创建部门',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `update_user` bigint DEFAULT NULL COMMENT '更新人员',
  `update_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `status` int DEFAULT NULL,
  `is_deleted` int DEFAULT NULL COMMENT '是否已删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC COMMENT='设备点检标准表';
show create table `t_iot_equipment_inspection_stand`;
drop database emis_issue;

create database if not exists emis_issue;
use emis_issue;
drop table if exists `t_iot_equipment_inspection_stand`;
CREATE TABLE `t_iot_equipment_inspection_stand` (
  `id` bigint NOT NULL,
  `tenant_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '租户ID',
  `equipment_category_id` bigint NOT NULL COMMENT '设备分类ID',
  `equipment_model_id` bigint NOT NULL,
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '点检项名称',
  `category` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '检验分类：定性、定量、智能点检',
  `inspection_content` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检内容',
  `inspection_tool` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检工具',
  `inspection_method` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '点检方法',
  `judgment_criteria` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '判断标准',
  `inspection_exceptions` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '异常描述[\\"表面有磨损\\",\\"表面有裂痕\\"]',
  `major` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '点检专业',
  `cycle_unit` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '执行周期,一次、年、季度、月、周、日、班次、小时',
  `cycle_num` decimal(20,6) DEFAULT NULL COMMENT '周期间隔',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '备注',
  `create_org` bigint DEFAULT NULL COMMENT '创建组织',
  `use_org` bigint DEFAULT NULL COMMENT '使用组织',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `create_user` bigint DEFAULT NULL COMMENT '创建人员',
  `create_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_dept` bigint DEFAULT NULL COMMENT '创建部门',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `update_user` bigint DEFAULT NULL COMMENT '更新人员',
  `update_user_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `status` int DEFAULT NULL,
  `is_deleted` int DEFAULT NULL COMMENT '是否已删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC COMMENT='设备点检标准表';
show create table `t_iot_equipment_inspection_stand`;
drop database emis_issue;
