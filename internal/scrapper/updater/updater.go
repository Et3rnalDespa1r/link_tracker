package updater

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"

	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/parser"
)

type Repository interface {
	GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]coreDomain.Link, error)
	UpdateLink(ctx context.Context, link coreDomain.Link) error
	GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error)
}

type Updater struct {
	botClient    *client.BotClient
	repo         Repository
	cfg          *config.Config
	logger       *slog.Logger
	githubClient *client.GithubClient
	soClient     *client.StackOverflowClient
}

func NewUpdater(
	botClient *client.BotClient,
	repo Repository,
	cfg *config.Config,
	logger *slog.Logger,
	githubClient *client.GithubClient,
	soClient *client.StackOverflowClient,
) *Updater {
	return &Updater{
		botClient:    botClient,
		repo:         repo,
		cfg:          cfg,
		logger:       logger,
		githubClient: githubClient,
		soClient:     soClient,
	}
}

func (u *Updater) ProcessUpdates(ctx context.Context) {
	u.logger.Info("Начата проверка обновлений по расписанию...")

	before := time.Now().Add(-u.cfg.CheckInterval)

	links, err := u.repo.GetLinksToCheck(ctx, u.cfg.BatchSize, before)
	if err != nil {
		u.logger.Error("Ошибка получения ссылок из БД", "error", err)
		return
	}

	if len(links) == 0 {
		return
	}

	u.logger.Info("Взят батч ссылок на проверку", slog.Int("count", len(links)))

	linksCh := make(chan coreDomain.Link, len(links))
	var wg sync.WaitGroup

	for i := 0; i < u.cfg.WorkerCount; i++ {
		wg.Add(1)
		go u.worker(ctx, &wg, linksCh, i)
	}

	for _, link := range links {
		linksCh <- link
	}
	close(linksCh)

	wg.Wait()
	u.logger.Info("Проверка батча обновлений завершена")
}

func (u *Updater) worker(ctx context.Context, wg *sync.WaitGroup, linksCh <-chan coreDomain.Link, workerID int) {
	defer wg.Done()

	for link := range linksCh {
		if err := u.processLink(ctx, link); err != nil {
			u.logger.Error("Ошибка обработки ссылки", "worker_id", workerID, "url", link.URL, "error", err)
		}
	}
}

func (u *Updater) processLink(ctx context.Context, link coreDomain.Link) error {
	u.logger.Debug("Проверка ссылки", "url", link.URL)

	parsedURL, err := parser.ParseURL(link.URL)
	if err != nil {
		u.updateCheckTime(ctx, link)
		return fmt.Errorf("ошибка парсинга URL: %w", err)
	}

	var updateDetail *scrapDomain.UpdateDetail

	switch parsedURL.Type {
	case parser.TypeGithub:
		updateDetail, err = u.githubClient.FetchLatestEvent(ctx, parsedURL.Owner, parsedURL.Repo, link.LastCheckTime)
	case parser.TypeStackOverflow:
		updateDetail, err = u.soClient.FetchLatestAnswer(ctx, parsedURL.QuestionID, link.LastCheckTime)
	}

	if err != nil {
		u.updateCheckTime(ctx, link)
		return fmt.Errorf("ошибка запроса к API внешнего сервиса: %w", err)
	}

	if updateDetail != nil {
		chatIDs, err := u.repo.GetChatIDsForLink(ctx, link.ID)
		if err != nil {
			return fmt.Errorf("ошибка получения chat_id: %w", err)
		}

		if len(chatIDs) > 0 {
			// Клеим строку из деталей
			description := fmt.Sprintf(
				"**Новое обновление!** (%s)\n\n"+
					"**Тема:** %s\n"+
					"**Автор:** %s\n"+
					"**Время:** %s\n\n"+
					"**Превью:**\n%s",
				updateDetail.Type,
				updateDetail.Title,
				updateDetail.Author,
				updateDetail.CreatedAt.Format("02.01.2006 15:04"),
				updateDetail.Preview,
			)

			updateReq := scrapDomain.LinkUpdate{
				ID:          link.ID,
				URL:         link.URL,
				Description: description,
				TgChatIDs:   chatIDs,
			}

			if err := u.botClient.SendUpdate(ctx, updateReq); err != nil {
				u.logger.Error("Не удалось отправить обновление боту", "error", err)
			} else {
				u.logger.Info("Обновление успешно отправлено", "url", link.URL, "chats", len(chatIDs))
			}
		}

		link.UpdatedAt = updateDetail.CreatedAt
	}

	return u.updateCheckTime(ctx, link)
}

func (u *Updater) updateCheckTime(ctx context.Context, link coreDomain.Link) error {
	link.LastCheckTime = time.Now()
	return u.repo.UpdateLink(ctx, link)
}
