CREATE TABLE `event_store` (
  `timestamp` bigint(20) unsigned NOT NULL,
  `aggregate_id` binary(16) NOT NULL,--is an UUID
  `version` mediumint(9) NOT NULL,
  `event_type` varchar(40) NOT NULL,
  `event_data` json NOT NULL,
  `aggregate_type` varchar (40) NOT NULL,
  PRIMARY KEY (`timestamp`,`aggregate_id`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `aggregates` (
  `aggregate_id` binary(16) NOT NULL,--is an UUID
  `version` mediumint(9) NOT NULL,
  PRIMARY KEY (`aggregate_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
