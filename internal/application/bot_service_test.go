package application

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
)

type mockSender struct{}

func (m *mockSender) SendMessage(chatID int64, text string) error {
	return nil
}

func setupTestService() *BotService {
	repo := repository.NewInMemoryLinkRepository()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sender := &mockSender{}
	return NewBotService(repo, sender, logger)
}

func TestBotService_TrackFlow_HappyPath(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	reply := svc.HandleCommand(ctx, chatID, CmdTrack, "")
	assert.Contains(t, reply, "Отправьте ссылку")
	assert.Equal(t, domain.StateWaitingForLink, svc.states[chatID])

	reply = svc.HandleText(ctx, chatID, "https://github.com/test/repo")
	assert.Contains(t, reply, "отправьте теги")
	assert.Equal(t, domain.StateWaitingForTags, svc.states[chatID])

	reply = svc.HandleText(ctx, chatID, "работа, пет-проект")
	assert.Equal(t, ReplyTrackSuccess, reply)
	assert.Equal(t, domain.StateNone, svc.states[chatID])

	reply = svc.HandleCommand(ctx, chatID, CmdList, "")
	assert.Contains(t, reply, "https://github.com/test/repo")
	assert.Contains(t, reply, "работа, пет-проект")
}

func TestBotService_TrackFlow_InvalidLink(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	svc.HandleCommand(ctx, chatID, CmdTrack, "")

	reply := svc.HandleText(ctx, chatID, "просто текст")
	assert.Equal(t, ReplyInvalidLink, reply)
	assert.Equal(t, domain.StateWaitingForLink, svc.states[chatID])
}

func TestBotService_CancelCommand(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	svc.HandleCommand(ctx, chatID, CmdTrack, "")

	reply := svc.HandleCommand(ctx, chatID, CmdCancel, "")
	assert.Equal(t, ReplyCancel, reply)

	assert.Equal(t, domain.StateNone, svc.states[chatID])
}
