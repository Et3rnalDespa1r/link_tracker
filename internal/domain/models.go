package domain

import "time"

type Link struct {
	ID            int64
	URL           string
	Tags          []string
	LastCheckTime time.Time
	UpdatedAt     time.Time
}

type UserState string

const (
	StateNone           UserState = "NONE"
	StateWaitingForLink UserState = "WAITING_FOR_LINK"
	StateWaitingForTags UserState = "WAITING_FOR_TAGS"
)

type LinkUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}
