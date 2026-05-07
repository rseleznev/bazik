package main

type Balancer struct {
	conf *Config
	servers []*Server
	conns []*Conn
}

func NewBalancer(c *Config) *Balancer {
	return &Balancer{
		conf: c,
	}
}

func (b *Balancer) Start() {
	// создать сокет
	// bind()
	// listen()

	// polling
	for {
		// <-ResultChan
		// clientSocket := accept()
		// go b.process(clientSocket)
	}
}

func (b *Balancer) process(clientSocket int) {
	// подбираем сервер
	s := b.findServer()
	// подключаемся к серверу
	serverSocket := s.Connect()

	// определяем адрес клиента
	cAddr := Address{}

	// создаем соединение
	conn := &Conn{
		clientAddr: cAddr,
		clientSock: clientSocket,

		server: s,
		serverSock: serverSocket,
	}

	b.conns = append(b.conns, conn)

	conn.process()
}

func (b *Balancer) findServer() *Server {
	return b.servers[0]
}