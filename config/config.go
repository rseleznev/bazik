package config

type Config struct {	
	IP string
	IPVersion string // ipv4
	Port int
	Proto string

	// Алгоритм балансировки
	BalancingAlg string

	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	Timeout int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxClientsLimit int

	// Максимальное время бездействия клиента
	MaxIdleTime int

	// Список доступных серверов
	Servers []struct {
		Address string
		RetryAmount int
		Timeout int
		MaxClientsLimit int
		MaxIdleTime int
	}
}