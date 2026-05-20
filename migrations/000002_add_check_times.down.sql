ALTER TABLE links
    DROP COLUMN IF EXISTS last_check_time,
    DROP COLUMN IF EXISTS updated_at;