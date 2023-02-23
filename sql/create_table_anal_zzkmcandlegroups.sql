CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `zzitemid` BIGINT,
    `km_candleid` VARCHAR(20),
    `km_setid` INT,
    PRIMARY KEY(zzitemid, km_candleid, km_setid)
);