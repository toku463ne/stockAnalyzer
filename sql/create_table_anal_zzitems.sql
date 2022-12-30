CREATE TABLE IF NOT EXISTS #TABLENAME# (
    zzitemid INT NOT NULL AUTO_INCREMENT,
    codename VARCHAR(50),
    startep INT,
    endep INT,
    km_groupid VARCHAR(50),
    #XYCOLUMS#,
    last_dir TINYINT,
    PRIMARY KEY(zzitemid),
    INDEX (codename)
);