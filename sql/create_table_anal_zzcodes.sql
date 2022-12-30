CREATE TABLE IF NOT EXISTS anal_zzcodes (
    `codename` VARCHAR(50),
    `obsyear` INT,
    `market` VARCHAR(50),
    `nbars` INT,
    `min_nth_volume` INT,
    PRIMARY KEY(codename, obsyear)
);