package linker

import "github.com/rseleznev/bazik/internal/models"

type chat struct {
	id string
	l *Linker
	
	poller poller
	sys syscaller
	
	clientAddr models.Address
	clientSock int

	server models.Server
	serverSock int
}

func (c *chat) process() {
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