package config

type Config struct {	
	IP string
	Port int
	Proto string

	// Алгоритм балансировки
	BalancingAlg string

	// Частота проверки жизни серверов
	// Размеры буферов ядра

	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	MaxResponseSeconds int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxClientsLimit int

	// Максимальное время бездействия клиента
	MaxIdleSeconds int

	// Отключение пула серверных сокетов
	// По умолчанию false, то есть пул создается
	DisableServerSocksPool bool
	// Максимальные размеры пула сокетов для каждого сервера.
	// Должен быть больше 0. По умолчанию 10
	MaxServerSocksPoolLen int
	// Начальное количество сокетов в пуле для каждого сервера.
	// Количество может увеличиваться до MaxServerSocksPoolLen в зависимости
	// от нагрузки, и потом снова снижается до InitialServerSocksPoolLen.
	// Не должен быть больше MaxServerSocksPoolLen. По умолчанию 3
	InitialServerSocksPoolLen int

	// Список доступных серверов
	Servers []struct {
		Address string

		// Настройки для конкретного сервера
		RetryAmount int
		MaxResponseSeconds int
		MaxClientsLimit int // должен быть меньше или равен общему MaxClientsLimit
		MaxIdleSeconds int
		DisableServerSocksPool bool
		MaxServerSocksPoolLen int
		InitialServerSocksPoolLen int
	}
}