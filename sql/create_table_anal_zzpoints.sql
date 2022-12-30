CREATE TABLE IF NOT EXISTS anal_zzpoints (
    zzitemid INT AUTO_INCREMENT,
    zzpointid INT, 
    x FLOAT,
    y FLOAT,
    PRIMARY KEY(zzitemid, zzpointid),
    INDEX (zzitemid)
);