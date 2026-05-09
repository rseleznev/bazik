package models

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type Server struct {
	sock int
	
	Addr Address
	Opts ServerOptions
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

	// Максимальное время бездействия соединения прежде чем оно будет закрыто
	MaxChatIdleTime int
}

type Client struct {
	Sock int
	
	Addr Address
}