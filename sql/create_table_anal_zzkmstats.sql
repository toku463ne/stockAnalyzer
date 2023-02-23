CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_id` VARCHAR(60),
    `km_setid` INT,
    `item_cnt` BIGINT,
    `item_ucnt` BIGINT,
    `item_dcnt` BIGINT,
    `year_cnt` INT,
    `score1` FLOAT,
    `score2` FLOAT,
    `meanx` FLOAT,
    `meany` FLOAT,
    `stdx` FLOAT,
    `stdy` FLOAT,
    PRIMARY KEY(`km_id`, `km_setid`)
);