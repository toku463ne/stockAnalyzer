CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `codename` VARCHAR(50),
    `name` VARCHAR(100),
    `source` VARCHAR(100),
    `market` VARCHAR(50),
    `market_detail` VARCHAR(100),
    `market_type` VARCHAR(50),
    `industry33_code` VARCHAR(50) DEFAULT '-',
    PRIMARY KEY(codename)
);