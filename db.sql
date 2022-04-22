-- Ali Fattahi , Ali.Robocup@gmail.com , alifattahi.ir

--
-- Table structure for table `summaries_updates`
--

DROP TABLE IF EXISTS `summaries_updates`;
CREATE TABLE IF NOT EXISTS `summaries_updates` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `table_name` varchar(100) NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_name` (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- Table structure for table `transactions`
--

DROP TABLE IF EXISTS `transactions`;
CREATE TABLE IF NOT EXISTS `transactions` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `type` enum('incoming','outgoing','refund','') NOT NULL DEFAULT 'incoming',
  `amount` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- Table structure for table `transactions_summary`
--

DROP TABLE IF EXISTS `transactions_summary`;
CREATE TABLE IF NOT EXISTS `transactions_summary` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` int(10) UNSIGNED NOT NULL,
  `duration` enum('daily','weekly','monthly','annual') NOT NULL DEFAULT 'daily',
  `transaction_type` enum('incoming','outgoing','refund','') NOT NULL,
  `total_count` int(10) UNSIGNED NOT NULL,
  `total_amount` int(11) NOT NULL,
  `starts_at` datetime NOT NULL,
  `ends_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tr_summary_unq` (`user_id`,`duration`,`transaction_type`,`starts_at`,`ends_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DELIMITER $$
CREATE FUNCTION `summary_last_update`(`tbl_name` VARCHAR(200)) RETURNS timestamp
    DETERMINISTIC
BEGIN
	   DECLARE last_update TIMESTAMP;
	   DECLARE default_start TIMESTAMP;
	   
	   SET default_start = (SELECT DATE_SUB( CURRENT_TIMESTAMP ( ), INTERVAL 200 DAY ));
	   SET last_update =( SELECT updated_at FROM summaries_updates where `table_name` = tbl_name);
	   
	IF ISNULL(last_update) THEN
		INSERT INTO `summaries_updates` SET `table_name` = tbl_name , updated_at=default_start;
		RETURN default_start;
	ELSE
		RETURN last_update ;
	END IF;

END$$
DELIMITER ;



DELIMITER $$
CREATE PROCEDURE UpdateTransactionSummary()
BEGIN
	START TRANSACTION;
	
	INSERT INTO transactions_summary (user_id, transaction_type, total_count,total_amount,starts_at,ends_at)
	SELECT tr.user_id,tr.type,tr.transaction_count,tr.amount,tr.starts_at,tr.ends_at FROM(
		SELECT
			count(id) as transaction_count,
			sum(amount) as amount,
			user_id,
			type,
			DATE_FORMAT(CONCAT(date(`created_at`), ' 00:00:00'), '%Y/%m/%d %H:%i:%s') as starts_at,
			DATE_FORMAT(CONCAT(date(`created_at`), ' 23:59:59'), '%Y/%m/%d %H:%i:%s') as ends_at
		FROM
			transactions
		WHERE created_at BETWEEN 
			summary_last_update('transactions_summary') AND CURRENT_TIMESTAMP () 
		GROUP BY 
			user_id,
			type,
			DATE_FORMAT(CONCAT(date(`created_at`), ' 00:00:00'), '%Y/%m/%d %H:%i:%s'),
			DATE_FORMAT(CONCAT(date(`created_at`), ' 23:59:59'), '%Y/%m/%d %H:%i:%s')
      
	) as tr
	ON DUPLICATE KEY UPDATE 
	`total_count` = `total_count` + tr.transaction_count,
	`total_amount`=`total_amount`+ tr.amount;
	UPDATE summaries_updates set updated_at = CURRENT_TIMESTAMP() WHERE table_name='transactions_summary';
	
	COMMIT;
END $$
DELIMITER ;



SET GLOBAL event_scheduler = ON;
CREATE EVENT UpdateTransactionSummaryEvent
    ON SCHEDULE EVERY 60 SECOND
    DO
      CALL UpdateTransactionSummary();



