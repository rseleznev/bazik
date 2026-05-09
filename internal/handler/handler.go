package handler

import (
	"errors"
	"os"
	"syscall"

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
	ipVersion string // IPv4
	addr [4]byte
	port int

	listeningSock int

	sys syscaller
	poller poller
}

func NewHandler(conf *config.Config) *Handler {
	return &Handler{}
}

func (h *Handler) Listen(ch chan *models.Client) {
	// создает сокет
	s, err := h.sys.NewSocket(0, 0, 0)
	if err != nil {
		os.Exit(1)
	}

	addr := syscall.SockaddrInet4{
		Port: h.port,
		Addr: h.addr,
	}

	err = h.sys.Bind(s, &addr)
	if err != nil {
		os.Exit(1)
	}

	err = h.sys.Listen(s, 10) // 10 - ожидающие клиенты; дать возможность настройки в конфиге?
	if err != nil {
		os.Exit(1)
	}

	for {
		clientSock, clientAddrRaw, err := h.sys.Accept(s)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				// polling
				continue
			}
			os.Exit(1)
		}

		_ = clientAddrRaw
		var clientAddr models.Address // преобразование clientAddrRaw

		client := &models.Client{
			Sock: clientSock,
			Addr: clientAddr,
		}

		ch <- client
	}
}

func (h *Handler) TCPProxy(client *models.Client, server *models.Server) error {
	// добавляет clientSock в epoll на входящие
	// добавляет serverSock в epoll на входящие

	for {
		// ждет события
		// select {
		// case <-clientSock.ResultChan:
			// копирует данные из клиентского сокета в серверный
			// syscall.Splice()
		// case <-serverSock.ResultChan:
			// копирует данные из серверного сокета в клиентский
			// syscall.Splice()
		// }
		// continue
		break
	}

	return nil
}