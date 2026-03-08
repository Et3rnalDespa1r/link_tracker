package domain

type Link struct {
	URL  string
	Tags []string
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
