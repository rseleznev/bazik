package config

type Config struct {	
	IP string
	IPVersion string // ipv4
	Port int
	Proto string

	// Алгоритм балансировки
	BalancingAlg string

	// Частота проверки жизни серверов

	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	MaxResponseTime int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxClientsLimit int

	// Максимальное время бездействия клиента
	MaxIdleTime int

	// Максимальное количество сокетов для каждого сервера
	MaxServerSocksPoolLen int
	// Начальное количество сокетов для каждого сервера
	// Количество может увеличиваться до MaxServerSocksPoolLen в зависимости
	// от нагрузки, и потом снова снижается до InitialServerSocksPoolLen
	InitialServerSocksPoolLen int

	// Список доступных серверов
	Servers []struct {
		Address string

		// Настройки для конкретного сервера
		RetryAmount int
		Timeout int
		MaxClientsLimit int
		MaxIdleTime int
		MaxServerSocksPoolLen int
		InitialServerSocksPoolLen int
	}
}