package models

import "time"

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type Server struct {
	Addr Address
	Opts ServerOptions
	LastActivity time.Time
	LastHealthCheck time.Time
	ConnectionsLen int
}

type ServerOptions struct {
	// Количество попыток при неудаче прежде чем вернется ошибка
	RetryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	Timeout int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	MaxClientsLimit int
}

type Client struct {
	Sock int
	
	Addr Address
	LastActivity time.Time
}