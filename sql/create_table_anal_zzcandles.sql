CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `zzitemid` BIGINT,
    `codename` VARCHAR(50),
    `EP` INT,
    `dir` TINYINT,
    #OHLCVCOLUMS#,
    next_peak FLOAT,
    PRIMARY KEY(zzitemid)
);