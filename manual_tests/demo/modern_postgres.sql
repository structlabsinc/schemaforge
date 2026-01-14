CREATE TYPE status_enum AS ENUM ('active', 'archived');
CREATE TABLE events (id SERIAL, status status_enum, event_date DATE) PARTITION BY RANGE (event_date);
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
