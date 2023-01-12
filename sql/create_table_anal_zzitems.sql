CREATE TABLE IF NOT EXISTS #TABLENAME# (
    zzitemid INT NOT NULL AUTO_INCREMENT,
    codename VARCHAR(50),
    startep INT,
    endep INT,
    km_groupid VARCHAR(50),
    km_mode INT,
    #XYCOLUMS#,
    last_dir TINYINT,
    PRIMARY KEY(zzitemid),
    INDEX (codename)
);