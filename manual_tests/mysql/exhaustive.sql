-- MySQL Exhaustive Coverage Test
CREATE TABLE blog_posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    FULLTEXT INDEX idx_content (content)
) ENGINE=InnoDB 
  AUTO_INCREMENT=100 
  CHARACTER SET utf8mb4 
  COLLATE utf8mb4_unicode_ci 
  ROW_FORMAT=COMPRESSED;

CREATE TABLE cache_data (
    k VARCHAR(50) PRIMARY KEY,
    v BLOB
) ENGINE=MEMORY;
