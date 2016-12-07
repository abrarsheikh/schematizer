CREATE TABLE `data_source_target_mapping` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_source_id` INT(11) NOT NULL,
  `data_source_type` VARCHAR(255) NOT NULL,
  `data_target_id` INT(11) NOT NULL,
  `created_at` INT(11) NOT NULL,
  `updated_at` INT(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
