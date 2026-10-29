package domain

import "time"

type LinkResponse struct {
	Tags []string `json:"tags"`
	ID   int64    `json:"id"`
	URL  string   `json:"url"`
}

type ListLinksResponse struct {
	Links []LinkResponse `json:"links"`
	Size  int            `json:"size"`
}

type AddLinkRequest struct {
	Tags []string `json:"tags"`
	Link string   `json:"link"`
}

type RemoveLinkRequest struct {
	Link string `json:"link"`
}

type LinkUpdate struct {
	Author      string  `json:"author,omitempty" avro:"-"`
	ID          int64   `json:"id" avro:"id"`
	URL         string  `json:"url" avro:"url"`
	Description string  `json:"description" avro:"description"`
	TgChatIDs   []int64 `json:"tgChatIds" avro:"tgChatIds"`
}

type UpdateDetail struct {
	Type      string
	Title     string
	Author    string
	CreatedAt time.Time
	Preview   string
}
