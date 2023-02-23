CREATE TABLE IF NOT EXISTS anal_zzkmsetids (
    `km_setid` INT NOT NULL AUTO_INCREMENT,
    `km_setname` VARCHAR(40) UNIQUE,
    `obsyear` INT,
    `startep` INT,
    `endep` INT,
    PRIMARY KEY(km_setid)
);
