CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `codename` VARCHAR(50),
    `obsyear` INT,
    `market` VARCHAR(50),
    `nbars` INT,
    `min_nth_volume` BIGINT,
    PRIMARY KEY(codename, obsyear)
);