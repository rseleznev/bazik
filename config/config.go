package config

type Config struct {	
	IP string
	Port int
	Proto string

	// Алгоритм балансировки
	BalancingAlg string

	// Режим проксирования
	// 	- zero-copy - прямой перенос данных между сокетами без копирования в user space (возможна потеря данных)
	// 	- guaranteed delivery - данные копируются в user space, новые сообщения не принимаются, пока не будет получен ACK
	ProxyMode string

	// Время в миллисекундах, за которое должна выполняться каждая операция
	MainTimeout int

	// Частота проверки жизни серверов
	// Размеры буферов ядра

	// Общие настройки для всех серверов
	ServerOptions

	// Список доступных серверов
	Servers []struct {
		Address string

		// Настройки для конкретного сервера.
		// Имеют приоритет над общими
		ServerOptions
	}
}

type ServerOptions struct {
	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	MaxResponseSeconds int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие клиенты будут получать ошибку ECONNREFUSED,
	// пока кол-во активных соединений не будет уменьшено
	MaxClientsAmount int

	// Максимальное время бездействия соединения
	MaxIdleSeconds int

	// Отключение пула серверных сокетов
	// По умолчанию false, то есть пул создается
	DisableSocksPool bool
	// Максимальные размеры пула сокетов для каждого сервера.
	// Должен быть больше 0. По умолчанию 10
	MaxSocksPoolLen int
	// Начальное количество сокетов в пуле для каждого сервера.
	// Количество может увеличиваться до MaxServerSocksPoolLen в зависимости
	// от нагрузки, и потом снова снижается до InitialServerSocksPoolLen.
	// Не должен быть больше MaxServerSocksPoolLen. По умолчанию 3
	InitialSocksPoolLen int
}

func (c *Config) Init() {

}