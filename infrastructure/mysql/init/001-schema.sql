CREATE DATABASE IF NOT EXISTS gds_analytics
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE gds_analytics;

CREATE TABLE IF NOT EXISTS metric_publications (
  publication_id CHAR(36) NOT NULL,
  source_hdfs_root VARCHAR(512) NOT NULL,
  output_version VARCHAR(32) NOT NULL,
  source_row_count INT UNSIGNED NOT NULL,
  successful_response_records BIGINT UNSIGNED NOT NULL,
  success_token_count BIGINT UNSIGNED NOT NULL,
  metrics_sha256 CHAR(64) NOT NULL,
  status ENUM('preparing', 'published', 'failed', 'unchanged') NOT NULL,
  failure_message VARCHAR(1024) NULL,
  started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  completed_at TIMESTAMP(6) NULL,
  PRIMARY KEY (publication_id),
  INDEX idx_publication_source_hash (
    source_hdfs_root(191), output_version, metrics_sha256, status
  ),
  INDEX idx_publication_completed (completed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS hourly_airline_metrics (
  stat_date DATE NOT NULL,
  stat_hour TINYINT UNSIGNED NOT NULL,
  airline_code VARCHAR(8) NOT NULL,
  successful_response_records BIGINT UNSIGNED NOT NULL,
  success_token_count BIGINT UNSIGNED NOT NULL,
  publication_id CHAR(36) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (stat_date, stat_hour, airline_code),
  CONSTRAINT chk_metrics_hour CHECK (stat_hour BETWEEN 0 AND 23),
  CONSTRAINT chk_metrics_token_count CHECK (
    success_token_count >= successful_response_records
  ),
  CONSTRAINT fk_metrics_publication FOREIGN KEY (publication_id)
    REFERENCES metric_publications(publication_id),
  INDEX idx_metrics_airline_time (airline_code, stat_date, stat_hour),
  INDEX idx_metrics_time_responses (
    stat_date, stat_hour, successful_response_records
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS hourly_airline_metrics_staging (
  stat_date DATE NOT NULL,
  stat_hour TINYINT UNSIGNED NOT NULL,
  airline_code VARCHAR(8) NOT NULL,
  successful_response_records BIGINT UNSIGNED NOT NULL,
  success_token_count BIGINT UNSIGNED NOT NULL,
  publication_id CHAR(36) NOT NULL,
  PRIMARY KEY (stat_date, stat_hour, airline_code),
  CONSTRAINT chk_staging_hour CHECK (stat_hour BETWEEN 0 AND 23),
  CONSTRAINT chk_staging_token_count CHECK (
    success_token_count >= successful_response_records
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
