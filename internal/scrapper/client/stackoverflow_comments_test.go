package client

import (
	"context"
	"github.com/stretchr/testify/require"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStackOverflowCommentContainsQuestionTitle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/answers"):
			w.Write([]byte(`{"items":[]}`))
		case strings.HasSuffix(r.URL.Path, "/comments"):
			w.Write([]byte(`{"items":[{"creation_date":2000,"body":"new comment","owner":{"display_name":"alice"}}]}`))
		default:
			w.Write([]byte(`{"items":[{"title":"Question title"}]}`))
		}
	}))
	defer server.Close()
	client := NewStackOverflowClient(server.URL, &config.Config{HTTPTimeout: time.Second, RetryAttempts: 1, CBMinimumRequiredCalls: 5})
	update, err := client.FetchLatestAnswer(context.Background(), "123", time.Unix(1000, 0))
	require.NoError(t, err)
	require.NotNil(t, update)
	require.Equal(t, "StackOverflow Comment", update.Type)
	require.Equal(t, "Question title", update.Title)
	require.Equal(t, "alice", update.Author)
}
