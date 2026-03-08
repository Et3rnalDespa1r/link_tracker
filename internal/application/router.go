package application

const (
	CmdStart = "start"
	CmdHelp  = "help"

	DescStart = "Начать работу с ботом"
	DescHelp  = "Показать список доступных команд"
)

const (
	ReplyStart   = "Добро пожаловать в LinkTracker! Используйте /help, чтобы посмотреть доступные команды."
	ReplyHelp    = "Доступные команды:\n/start - Начало работы\n/help - Список команд"
	ReplyUnknown = "Неизвестная команда. Воспользуйтесь /help, чтобы посмотреть список доступных команд."
)

func RouteCommand(command string) string {
	switch command {
	case CmdStart:
		return ReplyStart
	case CmdHelp:
		return ReplyHelp
	default:
		return ReplyUnknown
	}
}
