CREATE TABLE IF NOT EXISTS tick_tableeps (
    table_name VARCHAR(50),
    codename VARCHAR(50),
    startep INT,
    endep INT,
    startdt DATETIME,
    enddt DATETIME,
    PRIMARY KEY (table_name, codename)
);