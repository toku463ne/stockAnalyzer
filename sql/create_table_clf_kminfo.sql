CREATE TABLE IF NOT EXISTS clf_kminfo (
    `clf_id` INT,
    `km_id` VARCHAR(40),
    `score` FLOAT,
    `expected` FLOAT,
    `cnt` INT,
    PRIMARY KEY(clf_id, km_id)
);