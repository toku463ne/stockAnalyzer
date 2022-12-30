CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_groupid` VARCHAR(50),
    `count` INT,
    `lose_count` INT,
    `meanx` FLOAT,
    `meany` FLOAT,
    `stdx` FLOAT,
    `stdy` FLOAT,
    `last_epoch` FLOAT,
    #XYCOLUMS#,
    PRIMARY KEY(`km_groupid`)
);