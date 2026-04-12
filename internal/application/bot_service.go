package application

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

const (
	CmdStart   = "start"
	CmdHelp    = "help"
	CmdTrack   = "track"
	CmdUntrack = "untrack"
	CmdList    = "list"
	CmdCancel  = "cancel"

	DescStart = "Начать работу с ботом"
	DescHelp  = "Показать список доступных команд"

	ReplyCancel       = "Действие отменено."
	ReplyInvalidLink  = "Некорректная ссылка. Пожалуйста, отправьте валидный URL (например, https://github.com/user/repo) или /cancel для отмены."
	ReplyAlreadyTrack = "Ссылка уже отслеживается."
	ReplyTrackSuccess = "Ссылка успешно добавлена в список отслеживаемых!"
	ReplyNoLinks      = "У вас нет активных подписок."
	ReplyUnknown      = "Неизвестная команда. Воспользуйтесь /help, чтобы посмотреть список доступных команд."
)

type MessageSender interface {
	SendMessage(chatID int64, text string) error
}

type BotService struct {
	repo      LinkRepository
	sender    MessageSender
	states    map[int64]domain.UserState
	tempLinks map[int64]string
	logger    *slog.Logger
}

func NewBotService(repo LinkRepository, sender MessageSender, logger *slog.Logger) *BotService {
	return &BotService{
		repo:      repo,
		sender:    sender,
		states:    make(map[int64]domain.UserState),
		tempLinks: make(map[int64]string),
		logger:    logger,
	}
}

func (s *BotService) Notify(ctx context.Context, update domain.LinkUpdate) error {
	for _, chatID := range update.TgChatIDs {
		msg := fmt.Sprintf("🔔 Обновление по ссылке: %s\n%s", update.URL, update.Description)
		if err := s.sender.SendMessage(chatID, msg); err != nil {
			s.logger.Error("Не удалось отправить уведомление", "chat_id", chatID, "error", err)
		}
	}
	return nil
}

func (s *BotService) HandleCommand(ctx context.Context, chatID int64, command, args string) string {
	s.states[chatID] = domain.StateNone
	delete(s.tempLinks, chatID)

	switch command {
	case CmdStart:
		return "Добро пожаловать в LinkTracker! Используйте /help для просмотра команд."
	case CmdHelp:
		return "Доступные команды:\n/start - Начало работы\n/help - Справка\n/track - Отслеживать ссылку\n/untrack - Перестать отслеживать\n/list - Список ссылок\n/cancel - Отменить текущее действие"
	case CmdCancel:
		return ReplyCancel
	case CmdTrack:
		s.states[chatID] = domain.StateWaitingForLink
		return "Отправьте ссылку, которую хотите отслеживать (или /cancel для отмены):"
	case CmdUntrack:
		if args == "" {
			return "Укажите ссылку для удаления. Пример: /untrack https://github.com/..."
		}
		if err := s.repo.RemoveLink(ctx, chatID, args); err != nil {
			s.logger.Error("Ошибка при удалении ссылки", "err", err)
			return "Произошла внутренняя ошибка при удалении ссылки. Попробуйте позже."
		}
		return "Ссылка удалена из отслеживаемых."
	case CmdList:
		return s.handleList(ctx, chatID, args)
	default:
		return ReplyUnknown
	}
}

func (s *BotService) HandleText(ctx context.Context, chatID int64, text string) string {
	state := s.states[chatID]

	switch state {
	case domain.StateWaitingForLink:
		return s.processLinkInput(ctx, chatID, text)
	case domain.StateWaitingForTags:
		return s.processTagsInput(ctx, chatID, text)
	default:
		return "Я не понимаю это сообщение. Воспользуйтесь /help."
	}
}

func (s *BotService) processLinkInput(ctx context.Context, chatID int64, text string) string {
	parsedURL, err := url.ParseRequestURI(text)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return ReplyInvalidLink
	}

	exists, err := s.repo.HasLink(ctx, chatID, text)
	if err != nil {
		s.logger.Error("Ошибка при проверке существования ссылки", "err", err)
		return "Произошла ошибка при обращении к базе данных. Попробуйте позже."
	}
	if exists {
		s.states[chatID] = domain.StateNone
		return ReplyAlreadyTrack
	}

	s.tempLinks[chatID] = text
	s.states[chatID] = domain.StateWaitingForTags
	return "Отлично! Теперь отправьте теги через запятую (например: работа, баг) или напишите 'нет', если теги не нужны:"
}

func (s *BotService) processTagsInput(ctx context.Context, chatID int64, text string) string {
	linkURL := s.tempLinks[chatID]
	var tags []string

	if strings.ToLower(strings.TrimSpace(text)) != "нет" {
		rawTags := strings.Split(text, ",")
		for _, t := range rawTags {
			tags = append(tags, strings.TrimSpace(t))
		}
	}

	if err := s.repo.AddLink(ctx, chatID, domain.Link{URL: linkURL, Tags: tags}); err != nil {
		s.logger.Error("Ошибка при сохранении ссылки", "err", err)
		return "Не удалось сохранить ссылку из-за внутренней ошибки."
	}

	s.states[chatID] = domain.StateNone
	delete(s.tempLinks, chatID)

	return ReplyTrackSuccess
}

func (s *BotService) handleList(ctx context.Context, chatID int64, tagFilter string) string {
	links, err := s.repo.GetLinks(ctx, chatID)
	if err != nil {
		s.logger.Error("Ошибка при получении списка ссылок", "err", err)
		return "Не удалось загрузить список ссылок. Попробуйте позже."
	}
	if len(links) == 0 {
		return ReplyNoLinks
	}

	var sb strings.Builder
	sb.WriteString("Ваши отслеживаемые ссылки:\n")
	count := 0

	for _, link := range links {
		if tagFilter != "" && !containsTag(link.Tags, tagFilter) {
			continue
		}
		tagsStr := strings.Join(link.Tags, ", ")
		sb.WriteString(fmt.Sprintf("- %s (Теги: [%s])\n", link.URL, tagsStr))
		count++
	}

	if count == 0 {
		return ReplyNoLinks
	}
	return sb.String()
}

func containsTag(tags []string, target string) bool {
	for _, t := range tags {
		if t == target {
			return true
		}
	}
	return false
}
