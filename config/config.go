package config

import "github.com/rseleznev/bazik/internal/models"

type Config struct {	
	IP string `yaml:"ip"`
	Port int `yaml:"port"`

	// Протокол/уровень балансировки (tcp/udp/http)
	Proto string `yaml:"proto"`

	// Алгоритм балансировки
	BalancingAlg string `yaml:"balancing_alg"`

	// Режим проксирования
	//
	// 	- zero-copy - прямой перенос данных между сокетами без копирования в user space (возможна потеря данных)
	// 	- guaranteed delivery - данные копируются в user space, новые сообщения от отправителя 
	// 	не принимаются (поток в обратную сторону продолжает работу), пока не будет получен ACK
	ProxyMode string `yaml:"proxy_mode"`

	// Время в миллисекундах, за которое должна выполняться каждая операция
	MainTimeout int `yaml:"main_timeout"`
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
	RetryAmount int `yaml:"retry_amount"`

	// Максимальное кол-во клиентов.
	//
	// Когда лимит будет превышен, последующие клиенты будут получать ошибку ECONNREFUSED,
	// пока кол-во активных соединений не будет уменьшено
	MaxClientsAmount int `yaml:"max_clients_amount"`

	// Максимальное время бездействия соединения
	MaxIdleSeconds int `yaml:"max_idle_seconds"`

	// Отключение пула серверных соединений
	// По умолчанию false, то есть пул создается
	DisableConnsPool bool `yaml:"disable_conns_pool"`
	// Максимальные размеры пула соединений для каждого сервера.
	// Должен быть больше 0. По умолчанию 10
	MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
	// Начальное количество соединений в пуле для каждого сервера.
	// Количество может увеличиваться до MaxServerConnsPoolLen в зависимости
	// от нагрузки, и потом снова снижается до InitialServerConnsPoolLen.
	// Не должен быть больше MaxServerConnsPoolLen. По умолчанию 3
	InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`

	// ------------------------------------

	// Список доступных серверов
	Servers []struct {
		Address string `yaml:"address"`

		RetryAmount int `yaml:"retry_amount"`
		MaxClientsAmount int `yaml:"max_clients_amount"`
		MaxIdleSeconds int `yaml:"max_idle_seconds"`
		DisableSocksPool bool `yaml:"disable_conns_pool"`
		MaxSocksPoolLen int `yaml:"max_conns_pool_len"`
		InitialSocksPoolLen int `yaml:"initial_conns_pool_len"`
	}
}

type BalancerConfig struct {
	Balancer *models.BalancerOptions
	Servers []*models.ServerOptions
}

func (c *Config) Parse(path string) []BalancerConfig {
	return nil
}