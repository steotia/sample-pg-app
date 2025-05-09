-- Drop table if it exists
DROP TABLE IF EXISTS projects;

-- Create the table with Spanner-compatible SQL
CREATE TABLE projects (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    create_time TIMESTAMPTZ NOT NULL,
    description TEXT,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

-- Add unique constraint separately
ALTER TABLE projects ADD CONSTRAINT uk_projects_name UNIQUE (name);