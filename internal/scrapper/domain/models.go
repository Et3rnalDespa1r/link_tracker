package domain

type LinkResponse struct {
	ID  int64  `json:"id"`
	URL string `json:"url"`
}

type ListLinksResponse struct {
	Links []LinkResponse `json:"links"`
	Size  int            `json:"size"`
}

type AddLinkRequest struct {
	Link string `json:"link"`
}

type RemoveLinkRequest struct {
	Link string `json:"link"`
}

type LinkUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}
