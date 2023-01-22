CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `zzitemid` BIGINT,
    `km_id` VARCHAR(20),
    `km_setid` VARCHAR(40),
    `obsyear` INT,
    PRIMARY KEY(zzitemid, km_id, km_setid)
);