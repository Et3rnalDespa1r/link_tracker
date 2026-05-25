package processor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

type RawUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	Author      string  `json:"author"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}

type ProcessedUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
	Priority    string  `json:"priority"`
}

type Processor struct {
	stopWords        []string
	excludedAuthors  []string
	minLength        int
	summaryThreshold int
	hfToken          string
	httpClient       *http.Client
}

func NewProcessor(stopWords, excludedAuthors []string, minLength, summaryThreshold int, hfToken string) *Processor {
	lowerStopWords := make([]string, len(stopWords))
	for i, word := range stopWords {
		lowerStopWords[i] = strings.ToLower(word)
	}

	return &Processor{
		stopWords:        lowerStopWords,
		excludedAuthors:  excludedAuthors,
		minLength:        minLength,
		summaryThreshold: summaryThreshold,
		hfToken:          hfToken,
		httpClient:       &http.Client{Timeout: 10 * time.Second},
	}
}

func (p *Processor) ProcessUpdate(raw RawUpdate) *ProcessedUpdate {
	if utf8.RuneCountInString(raw.Description) < p.minLength {
		return nil
	}

	for _, author := range p.excludedAuthors {
		if strings.EqualFold(raw.Author, author) {
			return nil
		}
	}

	lowerDesc := strings.ToLower(raw.Description)
	for _, word := range p.stopWords {
		if strings.Contains(lowerDesc, word) {
			return nil
		}
	}

	finalDesc := raw.Description
	if utf8.RuneCountInString(finalDesc) > p.summaryThreshold {
		finalDesc = p.summarize(finalDesc)
	}

	return &ProcessedUpdate{
		ID:          raw.ID,
		URL:         raw.URL,
		Description: finalDesc,
		TgChatIDs:   raw.TgChatIDs,
		Priority:    "HIGH",
	}
}

func (p *Processor) summarize(text string) string {
	if p.hfToken == "" {
		return p.fallbackSummarize(text)
	}

	reqBody, _ := json.Marshal(map[string]string{"inputs": "Summarize the following update in 2-3 sentences: " + text})
	req, err := http.NewRequest("POST", "https://api-inference.huggingface.co/models/facebook/bart-large-cnn", bytes.NewBuffer(reqBody))
	if err != nil {
		return p.fallbackSummarize(text)
	}

	req.Header.Set("Authorization", "Bearer "+p.hfToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return p.fallbackSummarize(text)
	}
	defer resp.Body.Close()

	var result []map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err == nil && len(result) > 0 {
		if summary, ok := result[0]["summary_text"]; ok {
			return summary + " (AI Summary)"
		}
	}

	return p.fallbackSummarize(text)
}

func (p *Processor) fallbackSummarize(text string) string {
	runes := []rune(text)
	if len(runes) > p.summaryThreshold {
		return string(runes[:p.summaryThreshold]) + "..."
	}
	return text
}
