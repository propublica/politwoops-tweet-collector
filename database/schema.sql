DROP TABLE IF EXISTS `tweets`;
CREATE TABLE `tweets` (
	`id` BIGINT UNSIGNED AUTO_INCREMENT,
	`user_name` VARCHAR(64),
	`content` VARCHAR(255),
	`deleted` TINYINT(1) NOT NULL DEFAULT 0,
	`created` DATETIME NOT NULL,
	`modified` DATETIME NOT NULL,
	`tweet` TEXT,
	PRIMARY KEY(`id`),
	KEY(`user_name`),
	KEY(`deleted`),
	KEY(`created`),
	KEY(`modified`)
) DEFAULT CHARSET=UTF8;
