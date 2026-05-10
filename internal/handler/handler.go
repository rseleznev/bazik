package handler

import (
	"errors"
	"log"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type syscaller interface {
	NewSocket(int, int, int) (int, error)
	CloseSocket(int)
	Bind(int, syscall.Sockaddr) error
	Listen(int, int) error
	Accept(int) (int, syscall.Sockaddr, error)
	Connect(int, syscall.Sockaddr) error
	Splice()
}

type poller interface {
	Add() error
}

type Handler struct {
	proto string
	addr [4]byte
	port int

	listeningSock int
	serverSocksPool map[string][]*serverSock

	sys syscaller
	poller poller
}

func NewHandler(conf *config.Config) *Handler {
	// парсим конфиг

	// создаем пул сокетов для каждого сервера
	for _, v := range conf.Servers {

	}
	
	return &Handler{}
}

func (h *Handler) Accept() *models.Client {
	if h.listeningSock == 0 {
		h.createListeningSock()
	}

	for {
		clientSock, clientAddrRaw, err := h.sys.Accept(h.listeningSock)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				// polling
				continue
			}
			log.Fatal(err)
		}

		_ = clientAddrRaw
		var clientAddr models.Address // преобразование clientAddrRaw

		return &models.Client{
			Sock: clientSock,
			Addr: clientAddr,
		}
	}
}

func (h *Handler) createListeningSock() {
	s, err := h.sys.NewSocket(0, 0, 0)
	if err != nil {
		log.Fatal(err)
	}

	addr := syscall.SockaddrInet4{
		Port: h.port,
		Addr: h.addr,
	}

	err = h.sys.Bind(s, &addr)
	if err != nil {
		log.Fatal(err)
	}

	err = h.sys.Listen(s, 10) // 10 - ожидающие клиенты; дать возможность настройки в конфиге?
	if err != nil {
		log.Fatal(err)
	}

	h.listeningSock = s
}

// Close закрывает клиентский сокет без соединения с сервером,
// т.к. лимит клиентов превышен
func (h *Handler) Close(client *models.Client) {
	h.sys.CloseSocket(client.Sock)
}

func (h *Handler) TCPProxy(client *models.Client, server *models.Server) error {
	// ищем серверный сокет в пуле
	// если нет - смотрим, можем ли создать еще подключение к серверу
	// если может - создаем
	// если не можем - ошибка
	
	// добавляет clientSock в epoll на входящие
	// добавляет serverSock в epoll на входящие

	for {
		// ждет события
		select {

		// пришло сообщение от клиента
		case <-clientSock.ResultChan:
			// копирует данные из клиентского сокета в серверный
			h.sys.Splice()

		// пришло сообщение от сервера
		case <-serverSock.ResultChan:
			// копирует данные из серверного сокета в клиентский
			h.sys.Splice()

		// кто-то ждет ответ (Config.MaxResponseTime)
		case <-ResponseTimeout:
			return err

		// молчание в эфире (Config.MaxIdleTime)
		case <-IdleTimeout:
			return err

		}
		continue
	}

	// соединение может завершиться следующими вариантами:
	// 1)Истек таймаут бездействия (Config.MaxIdleTime)
	// 2)Клиент не ответил за таймаут (Config.MaxResponseTime)
	// 3)
	// 3)Клиент закрыл соединение
	// 4)Сервер закрыл соединение
	// 5)Клиент ответил ошибкой
	// 6)Сервер ответил ошибкой

	return nil
}

type serverSock struct {
	status string // ready, running
	sock int
	lastActivity time.Time
}