CREATE TABLE IF NOT EXISTS clf_ids (
    `id` INT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(40) UNIQUE,
    `type_name` VARCHAR(40),
    `valid_after_epoch` INT,
    `valid_after_dt` DATETIME,
    `desc` VARCHAR(100),
    PRIMARY KEY(`id`)
);
