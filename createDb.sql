CREATE TABLE notifications (
    id BIGSERIAL,
    user_id BIGINT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
GRANT all on notifications to write;