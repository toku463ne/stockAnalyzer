CREATE DATABASE stockanalyzer CHARACTER SET 'utf8';
CREATE DATABASE stockanalyzer_test CHARACTER SET 'utf8';
CREATE USER IF NOT EXISTS 'stockuser'@'%' IDENTIFIED BY 'stockpass';
GRANT ALL PRIVILEGES ON stockanalyzer.* to 'stockuser'@'%';
GRANT ALL PRIVILEGES ON stockanalyzer_test.* to 'stockuser'@'%';

