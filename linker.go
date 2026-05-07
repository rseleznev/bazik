package main

type poller interface {
	Add() error
}

type conn struct {
	clientAddr Address
	clientSock int

	server *Server
	serverSock int
}

type Linker struct {
	b Balancer
	p poller
	
	listenSocket int

	conns []*conn
}

func NewLinker(b Balancer, p poller) (*Linker, error) {
	return &Linker{
		b: b,
		p: p,
	}, nil
}

func (l *Linker) Serve() {
	// создать сокет
	// bind()
	// listen()

	// polling
	for {
		// <-ResultChan
		// clientSocket := accept()
		go l.link(0) // l.process(clientSocket)
	}
}

func (l *Linker) link(clientSocket int) {
	// подбираем сервер
	s := l.b.findServer()
	// подключаемся к серверу
	serverSocket, _ := l.newServerConnection(s)

	// проблема исчерпания доступных портов?

	// определяем адрес клиента
	cAddr := Address{}

	// создаем соединение
	conn := &conn{
		clientAddr: cAddr,
		clientSock: clientSocket,

		server: s,
		serverSock: serverSocket,
	}

	l.conns = append(l.conns, conn)

	conn.process()
}

func (c *conn) process() {
	// добавляет clientSock в epoll на входящие
	// добавляет serverSock в epoll на входящие

	for {
		// ждет события
		select {
		// case <-clientSock.ResultChan:
			// копирует данные из клиентского сокета в серверный
			// syscall.Splice()
		// case <-serverSock.ResultChan:
			// копирует данные из серверного сокета в клиентский
			// syscall.Splice()
		}
		// continue
	}
}

func (l *Linker) newServerConnection(s *Server) (int, error) {
	return 0, nil
}