CREATE TABLE IF NOT EXISTS chats (
                                     id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS links (
                                     id BIGSERIAL PRIMARY KEY,
                                     url TEXT UNIQUE NOT NULL,
                                     last_check_time TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01 00:00:00Z',
                                     updated_at TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01 00:00:00Z'
);

CREATE TABLE IF NOT EXISTS subscriptions (
                                             chat_id BIGINT REFERENCES chats(id) ON DELETE CASCADE,
                                             link_id BIGINT REFERENCES links(id) ON DELETE CASCADE,
                                             PRIMARY KEY (chat_id, link_id)
);

CREATE TABLE IF NOT EXISTS tags (
                                    id BIGSERIAL PRIMARY KEY,
                                    chat_id BIGINT NOT NULL,
                                    link_id BIGINT NOT NULL,
                                    name TEXT NOT NULL,
                                    FOREIGN KEY (chat_id, link_id) REFERENCES subscriptions(chat_id, link_id) ON DELETE CASCADE,
                                    UNIQUE (chat_id, link_id, name)
);

CREATE TABLE IF NOT EXISTS link_updates_outbox (
                                                   id BIGSERIAL PRIMARY KEY,
                                                   payload JSONB NOT NULL,
                                                   status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                                                   created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_links_url ON links(url);
CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_id ON subscriptions(chat_id);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON link_updates_outbox (status) WHERE status = 'PENDING';