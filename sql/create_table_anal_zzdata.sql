CREATE TABLE IF NOT EXISTS #TABLENAME# (
    codename VARCHAR(50),
    EP INT NOT NULL,
    DT DATETIME, 
    P FLOAT,
    dir TINYINT,
    dist INT,
    PRIMARY KEY (codename, EP)
);