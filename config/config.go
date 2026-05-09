package config


type Config struct {
	Proto string
	
	IPVersion string // IPv4
	Addr string // 127.0.0.1:5000
	
	// Алгоритм балансировки
	BalancingAlg string

	// Список доступных серверов
	Servers []string

	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	Timeout int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxClientsLimit int

	// Максимальное время бездействия соединения прежде чем оно будет закрыто
	MaxChatIdleTime int
}