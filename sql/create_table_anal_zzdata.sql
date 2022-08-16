CREATE TABLE IF NOT EXISTS anal_zzdata (
    zzgroupid INT,
    EP INT NOT NULL,
    DT DATETIME, 
    P FLOAT,
    PRIMARY KEY (zzgroupid, EP),
    FOREIGN KEY (zzgroupid) REFERENCES anal_zzgroups(zzgroupid)
);