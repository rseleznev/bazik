package linker

import (
	"sync"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type router interface {
	FindServer() models.Server
}

type poller interface {
	Add() error
	Close()
}

type syscaller interface {
	NewSocket() (int, error)
	CloseSocket(int)
	Bind()
	Listen()
	Accept()
	Connect()
	Splice()
}

type Linker struct {
	mu sync.Mutex
	opts *config.ChatOptions
	listeningSock int
	chats map[string]*chat
	
	router router
	poller poller
	sys syscaller
}

func NewLinker(c *config.Config, r router, p poller) (*Linker, error) {
	o := &c.ChatOptions
	
	return &Linker{
		opts: o,
		mu: sync.Mutex{},
		router: r,
		poller: p,
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
		go l.link(0) // передается clientSocket
	}
}

func (l *Linker) link(clientSocket int) {
	l.mu.Lock()
	
	// подбираем сервер
	s := l.router.FindServer()
	// подключаемся к серверу
	serverSocket, _ := l.newServerConnection()

	// проблема исчерпания доступных портов?

	// определяем адрес клиента
	cAddr := models.Address{}

	// создаем соединение
	chat := &chat{
		id: "!!!",
		l: l,
		clientAddr: cAddr,
		clientSock: clientSocket,

		server: s,
		serverSock: serverSocket,
	}
	l.chats[chat.id] = chat

	l.mu.Unlock()

	chat.process()
}

func (l *Linker) newServerConnection() (int, error) {
	var serverSock int
	
	return serverSock, nil
}