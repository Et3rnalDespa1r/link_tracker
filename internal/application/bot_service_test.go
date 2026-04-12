package application

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
)

type mockSender struct{}

func (m *mockSender) SendMessage(chatID int64, text string) error {
	return nil
}

func setupTestService() *BotService {
	repo := repository.NewInMemoryLinkRepository()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	sender := &mockSender{}
	return NewBotService(repo, sender, logger)
}

func TestBotService_TrackFlow_HappyPath(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	reply := svc.HandleCommand(ctx, chatID, CmdTrack, "")
	assert.Contains(t, reply, "Отправьте ссылку")

	reply = svc.HandleText(ctx, chatID, "https://github.com/test")
	assert.Contains(t, reply, "отправьте теги")

	reply = svc.HandleText(ctx, chatID, "go, backend")
	assert.Contains(t, reply, "успешно добавлена")
}

func TestBotService_TrackFlow_InvalidLink(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	svc.HandleCommand(ctx, chatID, CmdTrack, "")

	reply := svc.HandleText(ctx, chatID, "просто текст")
	assert.Equal(t, ReplyInvalidLink, reply)

	reply = svc.HandleText(ctx, chatID, "https://github.com/test")
	assert.Contains(t, reply, "отправьте теги")
}

func TestBotService_CancelCommand(t *testing.T) {
	svc := setupTestService()
	ctx := context.Background()
	chatID := int64(12345)

	svc.HandleCommand(ctx, chatID, CmdTrack, "")

	reply := svc.HandleCommand(ctx, chatID, CmdCancel, "")
	assert.Equal(t, ReplyCancel, reply)

	reply = svc.HandleText(ctx, chatID, "случайный текст")
	assert.Contains(t, reply, "Я не понимаю это сообщение")
}
