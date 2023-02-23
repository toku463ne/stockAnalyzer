CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_setid` INT,
    `zzitemid` BIGINT,
    `km_id` VARCHAR(20),
    `km_candleid` VARCHAR(20),
    PRIMARY KEY(zzitemid, km_setid)
);