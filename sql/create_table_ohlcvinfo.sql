CREATE TABLE IF NOT EXISTS ohlcvinfo (
    `tablename` VARCHAR(100),
    `codename` VARCHAR(50),
    `granularity` VARCHAR(10),
    PRIMARY KEY(`tablename`)
);