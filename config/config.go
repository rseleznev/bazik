package config

import "github.com/rseleznev/bazik/internal/models"

type Config struct {
	// Алгоритм балансировки
	BalancingAlg string

	// Список доступных серверов
	Servers []ParsedServer

	// Общие настройки для всех соединений
	ChatOptions
}

type ParsedServer struct {
	// Адрес сервера
	Addr models.Address

	// Настройки конкретного сервера
	ChatOptions
}

type ChatOptions struct {
	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	Timeout int

	// Максимальное кол-во соединений.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxChatsLimit int

	// Максимальное время бездействия соединения прежде чем оно будет закрыто
	MaxChatIdleTime int
}