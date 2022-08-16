CREATE TABLE IF NOT EXISTS anal_zzpoints (
    zzitemid INT,
    zzpointid INT, 
    x FLOAT,
    y FLOAT,
    PRIMARY KEY(zzitemid, zzpointid),
    INDEX (zzitemid),
    FOREIGN KEY (zzgroupid) REFERENCES anal_zzgroups(zzgroupid)
);