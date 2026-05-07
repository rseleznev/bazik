package main

type Conn struct {
	clientAddr Address
	clientSock int

	server *Server
	serverSock int
}

func (c *Conn) process() {
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