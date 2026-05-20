package updater

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	scrapperDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type MockRepo struct {
	links []coreDomain.Link
}

func (m *MockRepo) GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]coreDomain.Link, error) {
	return m.links, nil
}
func (m *MockRepo) UpdateLink(ctx context.Context, link coreDomain.Link) error { return nil }
func (m *MockRepo) GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error) {
	return []int64{111}, nil
}

func (m *MockRepo) UpdateLinkAndSaveOutbox(ctx context.Context, link coreDomain.Link, update scrapperDomain.LinkUpdate) error {
	return m.UpdateLink(ctx, link)
}
func TestUpdater_ProcessUpdates_ErrorIsolation(t *testing.T) {
	updatesSent := 0
	mockBot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		updatesSent++
		w.WriteHeader(http.StatusOK)
	}))
	defer mockBot.Close()

	mockGithub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "fail") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`[{"type": "IssuesEvent", "created_at": "2099-01-01T00:00:00Z", "payload": {"action": "opened", "issue": {"title": "Ok"}}}]`))
	}))
	defer mockGithub.Close()

	botClient := client.NewBotClient(mockBot.URL)
	ghClient := client.NewGithubClient(mockGithub.URL, "")
	soClient := client.NewStackOverflowClient("")

	repo := &MockRepo{
		links: []coreDomain.Link{
			{ID: 1, URL: "https://github.com/owner/fail"},
			{ID: 2, URL: "https://github.com/owner/success"},
		},
	}

	cfg := &config.Config{BatchSize: 2, WorkerCount: 2}

	svc := NewUpdater(botClient, repo, cfg, slog.Default(), ghClient, soClient)
	svc.ProcessUpdates(context.Background())

	assert.Equal(t, 1, updatesSent, "Должно быть отправлено ровно 1 успешное обновление")
}
