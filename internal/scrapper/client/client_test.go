package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGithubClient_FetchLatestEvent(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{
				"type": "IssuesEvent",
				"created_at": "` + time.Now().Format(time.RFC3339) + `",
				"actor": {"login": "daniil"},
				"payload": {
					"action": "opened",
					"issue": {"title": "Test Issue", "body": "Short body"}
				}
			}
		]`))
	}))
	defer mockServer.Close()

	client := NewGithubClient(mockServer.URL, "")
	detail, err := client.FetchLatestEvent(context.Background(), "owner", "repo", time.Now().Add(-24*time.Hour))

	assert.NoError(t, err)
	assert.NotNil(t, detail)
	assert.Equal(t, "GitHub Issue", detail.Type)
	assert.Equal(t, "Test Issue", detail.Title)
	assert.Equal(t, "daniil", detail.Author)
}

func TestStackOverflowClient_FetchLatestAnswer(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"items": [
				{
					"answer_id": 123,
					"creation_date": ` + "2000000000" + `,
					"body": "This is a great answer",
					"owner": {"display_name": "SO_User"}
				}
			]
		}`))
	}))
	defer mockServer.Close()

	client := NewStackOverflowClient(mockServer.URL)
	detail, err := client.FetchLatestAnswer(context.Background(), "12345", time.Unix(0, 0))

	assert.NoError(t, err)
	assert.NotNil(t, detail)
	assert.Equal(t, "SO_User", detail.Author)
}

func TestClient_APIFailure(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	client := NewGithubClient(mockServer.URL, "")
	detail, err := client.FetchLatestEvent(context.Background(), "owner", "repo", time.Now())

	assert.Error(t, err)
	assert.Nil(t, detail)
}

func TestTruncateText(t *testing.T) {
	longText := strings.Repeat("А", 300)
	result := truncateText(longText, 200)

	assert.Equal(t, 203, len([]rune(result)))
	assert.True(t, strings.HasSuffix(result, "..."))
}
