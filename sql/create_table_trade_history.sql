CREATE TABLE IF NOT EXISTS trade_history (
    `trade_name` VARCHAR(50),
    `epoch` INT,
    `reference_datetime` datetime, 
    `order_id` VARCHAR(100),
    `codename` VARCHAR(50),
    `side` SMALLINT,
    `price` FLOAT,
    `units` INT,
    `buy_offline` FLOAT,
    `buy_online` FLOAT,
    `sell_offline` FLOAT,
    `sell_online` FLOAT,
    INDEX(trade_name, reference_datetime)
);