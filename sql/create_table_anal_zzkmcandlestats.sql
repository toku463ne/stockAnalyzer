CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_candleid` VARCHAR(60),
    `km_setid` INT,
    `item_cnt` BIGINT,
    `dpeak_cnt` BIGINT,
    `dtrend_cnt` BIGINT,
    `upeak_cnt` BIGINT,
    `utrend_cnt` BIGINT,
    `score` FLOAT,
    `mean` FLOAT,
    `std` FLOAT,
    PRIMARY KEY(`km_candleid`, `km_setid`)
);