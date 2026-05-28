package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	PriorityHigh   = "HIGH"
	PriorityMedium = "MEDIUM"
	PriorityLow    = "LOW"
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
	highKeywords     []string
	lowKeywords      []string
}

func NewProcessor(stopWords, excludedAuthors []string, minLength, summaryThreshold int, hfToken string, highKeywords, lowKeywords []string) *Processor {
	lowerStopWords := make([]string, len(stopWords))
	for i, word := range stopWords {
		lowerStopWords[i] = strings.ToLower(word)
	}
	lowerHigh := make([]string, len(highKeywords))
	for i, word := range highKeywords {
		lowerHigh[i] = strings.ToLower(word)
	}
	lowerLow := make([]string, len(lowKeywords))
	for i, word := range lowKeywords {
		lowerLow[i] = strings.ToLower(word)
	}

	return &Processor{
		stopWords:        lowerStopWords,
		excludedAuthors:  excludedAuthors,
		minLength:        minLength,
		summaryThreshold: summaryThreshold,
		hfToken:          hfToken,
		httpClient:       &http.Client{Timeout: 10 * time.Second},
		highKeywords:     lowerHigh,
		lowKeywords:      lowerLow,
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

	// FR-1: Приоритизация
	priority := PriorityMedium
	for _, word := range p.highKeywords {
		if strings.Contains(lowerDesc, word) {
			priority = PriorityHigh
			break
		}
	}
	if priority == PriorityMedium {
		for _, word := range p.lowKeywords {
			if strings.Contains(lowerDesc, word) {
				priority = PriorityLow
				break
			}
		}
	}

	// Суммаризация
	finalDesc := raw.Description
	if utf8.RuneCountInString(finalDesc) > p.summaryThreshold {
		finalDesc = p.summarize(finalDesc)
	}

	return &ProcessedUpdate{
		ID:          raw.ID,
		URL:         raw.URL,
		Description: finalDesc,
		TgChatIDs:   raw.TgChatIDs,
		Priority:    priority,
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

func maxPriority(p1, p2 string) string {
	if p1 == PriorityHigh || p2 == PriorityHigh {
		return PriorityHigh
	}
	if p1 == PriorityMedium || p2 == PriorityMedium {
		return PriorityMedium
	}
	return PriorityLow
}

type Grouper struct {
	window     time.Duration
	updates    []ProcessedUpdate
	mu         sync.Mutex
	outChan    chan []ProcessedUpdate
	flushTimer *time.Timer
}

func NewGrouper(window time.Duration) *Grouper {
	return &Grouper{
		window:  window,
		outChan: make(chan []ProcessedUpdate, 100),
	}
}

func (g *Grouper) Add(u ProcessedUpdate) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.updates = append(g.updates, u)

	if g.flushTimer == nil {
		g.flushTimer = time.AfterFunc(g.window, g.flush)
	}
}

func (g *Grouper) flush() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.updates) == 0 {
		g.flushTimer = nil
		return
	}

	groups := make(map[string][]ProcessedUpdate)
	for _, u := range g.updates {
		key := fmt.Sprint(u.TgChatIDs)
		groups[key] = append(groups[key], u)
	}

	var result []ProcessedUpdate
	for _, group := range groups {
		if len(group) == 1 {
			result = append(result, group[0])
			continue
		}

		var combinedDesc strings.Builder
		maxPrio := PriorityLow
		for i, u := range group {
			maxPrio = maxPriority(maxPrio, u.Priority)
			combinedDesc.WriteString(fmt.Sprintf("%d. %s\n", i+1, u.Description))
		}

		result = append(result, ProcessedUpdate{
			ID:          group[0].ID,
			URL:         group[0].URL,
			Description: strings.TrimSpace(combinedDesc.String()),
			TgChatIDs:   group[0].TgChatIDs,
			Priority:    maxPrio,
		})
	}

	g.updates = nil
	g.flushTimer = nil
	g.outChan <- result
}

func (g *Grouper) Out() <-chan []ProcessedUpdate {
	return g.outChan
}
