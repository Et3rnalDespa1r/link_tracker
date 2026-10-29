package http

import (
	"context"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestScrapperRepositoryPreservesTagsAndChat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "-123", r.Header.Get("Tg-Chat-Id"))
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"links":[{"id":7,"url":"https://github.com/a/b","tags":["work"]}],"size":1}`))
			return
		}
		require.Equal(t, "/tg-chat/-123", r.URL.Path)
	}))
	defer server.Close()
	repo := NewScrapperRepository(server.URL, time.Second)
	require.NoError(t, repo.RegisterChat(context.Background(), -123))
	links, err := repo.GetLinks(context.Background(), -123)
	require.NoError(t, err)
	require.Len(t, links, 1)
	require.Equal(t, int64(7), links[0].ID)
	require.Equal(t, []string{"work"}, links[0].Tags)
}
