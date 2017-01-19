CREATE TABLE `schema_alias` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `namespace_id` int(11) NOT NULL,
  `source_id` int(11) NOT NULL,
  `alias` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `namespace_id_source_id_alias` (`namespace_id`, `source_id`, `alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
