CREATE TABLE IF NOT EXISTS #TABLENAME# (
    `item_id` BIGINT,
    `clf_id` INT,
    `km_id` VARCHAR(40),
    PRIMARY KEY(item_id, clf_id)
);