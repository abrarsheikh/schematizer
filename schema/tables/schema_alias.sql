CREATE TABLE `schema_alias` (
  `id` INT(11) AUTO_INCREMENT NOT NULL,
  `source_id` INT(11) NOT NULL,
  `alias` VARCHAR(255) NOT NULL,
  `schema_id` INT(11) NOT NULL,
  `created_at` INT(11) NOT NULL,
  `updated_at` INT(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_id_alias_unique_constraint` (`source_id`, `alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
