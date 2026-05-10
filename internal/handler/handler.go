package handler

import (
	"errors"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type syscaller interface {
	NewSocket(int, int, int) (int, error)
	CloseSocket(int) error
	Bind(int, syscall.Sockaddr) error
	Listen(int, int) error
	Accept(int) (int, syscall.Sockaddr, error)
	Connect(int, syscall.Sockaddr) error
	Splice()
}

type poller interface {
	Add(models.PollingUnit) error
}

type Handler struct {
	mu sync.Mutex

	listeningSock int
	serverSocksPool map[string][]int

	sys syscaller
	poller poller
}

func NewHandler(p poller) *Handler {
	return &Handler{
		serverSocksPool: make(map[string][]int),
		sys: realSyscalls{},
		poller: p,
	}
}

func (h *Handler) InitServer(server models.Server) {

	var wg sync.WaitGroup
	for range server.InitialPoolLen() {
		wg.Go(func() {
			sock := h.createSock()
			h.connectServerSock(sock, server)
			h.addServerSockInPool(server.GetID(), sock)	
		})
	}
	wg.Wait()
}

func (h *Handler) createSock() int {
	s, err := h.sys.NewSocket(syscall.AF_INET, syscall.SOCK_STREAM | syscall.SOCK_NONBLOCK, syscall.IPPROTO_TCP)
	if err != nil {
		log.Fatal(err)
	}

	return s
}

func (h *Handler) connectServerSock(sock int, server models.Server) {
	addr := server.GetAddrIp4()
	
	err := h.sys.Connect(sock, &addr)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINPROGRESS) {
			h.poll(sock, "connect")
		}
		
		log.Fatal(err)
	}
}

func (h *Handler) poll(sock int, eventType string) {
	pUnit := models.PollingUnit{
		SocketFd: sock,
		EventType: eventType,
		ResultChan: make(chan error),
	}

	err := h.poller.Add(pUnit)
	if err != nil {

	}

	// нужно узнавать таймаут по sock
}

func (h *Handler) addServerSockInPool(key string, sock int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.serverSocksPool[key]) == 0 {
		h.serverSocksPool[key] = make([]int, 0, 10)
	}
	h.serverSocksPool[key] = append(h.serverSocksPool[key], sock)
}

func (h *Handler) Listen(addr models.Address) {
	s := h.createSock()

	err := h.sys.Bind(s, &syscall.SockaddrInet4{
		Port: addr.Port,
		Addr: addr.IP,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = h.sys.Listen(s, 10) // 10 - ожидающие клиенты; дать возможность настройки в конфиге?
	if err != nil {
		log.Fatal(err)
	}

	h.listeningSock = s
}

func (h *Handler) Accept() *models.Client {
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

// Close закрывает клиентский сокет без соединения с сервером,
// т.к. лимит клиентов превышен
func (h *Handler) Close(client *models.Client) {
	h.sys.CloseSocket(client.Sock)
}

func (h *Handler) TCPProxy(client *models.Client, server models.Server) error {
	// ищем серверный сокет в пуле
	// если нет - смотрим, можем ли создать еще подключение к серверу
	// если может - создаем
	// если не можем - ошибка
	
	// добавляет clientSock в epoll на входящие
	// добавляет serverSock в epoll на входящие

	// for {
	// 	// ждет события
	// 	select {

	// 	// пришло сообщение от клиента
	// 	case <-clientSock.ResultChan:
	// 		// копирует данные из клиентского сокета в серверный
	// 		h.sys.Splice()

	// 	// пришло сообщение от сервера
	// 	case <-serverSock.ResultChan:
	// 		// копирует данные из серверного сокета в клиентский
	// 		h.sys.Splice()

	// 	// кто-то ждет ответ (Config.MaxResponseTime)
	// 	case <-ResponseTimeout:
	// 		return err

	// 	// молчание в эфире (Config.MaxIdleTime)
	// 	case <-IdleTimeout:
	// 		return err

	// 	}
	// 	continue
	// }

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