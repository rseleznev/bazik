package config

import "github.com/rseleznev/bazik/internal/models"

type Config struct {	
	IP string
	Port int

	// Протокол/уровень балансировки (tcp/udp/http)
	Proto string

	// Алгоритм балансировки
	BalancerAlg string

	// Режим проксирования
	//
	// 	- zero-copy - прямой перенос данных между сокетами без копирования в user space (возможна потеря данных)
	// 	- guaranteed delivery - данные копируются в user space, новые сообщения от отправителя 
	// 	не принимаются (поток в обратную сторону продолжает работу), пока не будет получен ACK
	ProxyMode string

	// Время в миллисекундах, за которое должна выполняться каждая операция
	MainTimeout int
	// Время в миллисекундах, за которое должен прийти ACK на отправленный пакет (TCP_USER_TIMEOUT)
	//
	// 0 - системный дефолт (15-20 минут)
	TCP_ACK_Timeout int

	// Частота проверки жизни серверов
	// Размеры буферов ядра

	// ------------------------------------
	// Общие настройки для всех серверов
	// У серверов аналогичные настройки, которые имеют приоритет над общими
	//

	// Количество попыток переподключиться к серверу при ошибке.
	//
	// Если необходима целостность данных, следует установить 0,
	// в таком случае при первой же ошибке соединение с клиентом будет закрыто
	// и клиент должен будет начать передачу заново
	//
	// Если RetryAmount >= 1, может нарушиться целостность данных, т.к.
	// сначала будут переданы те данные, которые остались с неудачной попытки
	RetryAmount int

	// Максимальное кол-во клиентов.
	//
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

	// ------------------------------------

	// Список доступных серверов
	Servers []struct {
		Address string

		RetryAmount int
		MaxClientsAmount int
		MaxIdleSeconds int
		DisableSocksPool bool
		MaxSocksPoolLen int
		InitialSocksPoolLen int
	}
}

type BalancerConfig struct {
	Balancer *models.BalancerOptions
	Servers []*models.ServerOptions
}

func (c *Config) Parse(path string) []BalancerConfig {
	return nil
}