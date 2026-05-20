CREATE TABLE IF NOT EXISTS chats (
                                     id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS links (
                                     id BIGSERIAL PRIMARY KEY,
                                     url TEXT UNIQUE NOT NULL
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

CREATE INDEX idx_links_url ON links(url);
CREATE INDEX idx_subscriptions_chat_id ON subscriptions(chat_id);