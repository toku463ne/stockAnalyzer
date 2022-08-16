CREATE TABLE IF NOT EXISTS #TABLENAME# (
    zzitemid INT NOT NULL AUTO_INCREMENT,
    zzgroupid INT NOT NULL,
    startep INT,
    endep INT,
    km_groupid INT,
    #XYCOLUMS#,
    PRIMARY KEY(zzitemid),
    INDEX (zzgroupid),
    FOREIGN KEY (zzgroupid) REFERENCES anal_zzgroups(zzgroupid)
);