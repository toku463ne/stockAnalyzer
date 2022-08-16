CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `km_groupid` INT NOT NULL,
    `count` INT,
    `mean` FLOAT,
    `std` FLOAT,
    `last_epoch` FLOAT,
    #XYCOLUMS#,
    PRIMARY KEY(`km_groupid`)
);