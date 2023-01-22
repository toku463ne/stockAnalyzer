CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_id` VARCHAR(60),
    `km_setid` VARCHAR(40),
    `count` INT,
    `peak_count` INT,
    `meanx` FLOAT,
    `meany` FLOAT,
    `stdx` FLOAT,
    `stdy` FLOAT,
    `last_epoch` FLOAT,
    #XYCOLUMS#,
    PRIMARY KEY(`km_id`, `km_setid`)
);