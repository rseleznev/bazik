package handler

import (
	"errors"
	"log"
	"runtime"
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
	DeleteSocketFromPolling(int)
}

type Handler struct {
	mu sync.RWMutex
	cancelCh chan struct{}

	listeningSock int
	serverSocksPool map[string][]int
	socksTimeout map[int]time.Duration

	sys syscaller
	poller poller
}

func NewHandler(p poller) *Handler {
	return &Handler{
		mu: sync.RWMutex{},
		cancelCh: make(chan struct{}),
		serverSocksPool: make(map[string][]int),
		socksTimeout: make(map[int]time.Duration),
		sys: realSyscalls{},
		poller: p,
	}
}

// ------------------------------------------------
// Методы с блокировкой
// ------------------------------------------------

func (h *Handler) addServerSockInPool(key string, sock int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.serverSocksPool[key]) == 0 {
		h.serverSocksPool[key] = make([]int, 0, 10)
	}
	h.serverSocksPool[key] = append(h.serverSocksPool[key], sock)
}

func (h *Handler) getServerSockFromPool(key string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	serverSocks := h.serverSocksPool[key]
	if serverSocks == nil {
		return 0
	}
	if len(serverSocks) == 0 {
		return 0
	}

	s := serverSocks[len(serverSocks)-1]
	h.serverSocksPool[key] = serverSocks[:len(serverSocks)-1]
	return s
}

func (h *Handler) addTimeoutForSock(sock int, timeout time.Duration) {
	h.mu.Lock()
	h.socksTimeout[sock] = timeout
	h.mu.Unlock()
}

func (h *Handler) getTimeoutForSock(sock int) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.socksTimeout[sock]
}

func (h *Handler) setListeningSock(sock int) {
	h.mu.Lock()
	h.listeningSock = sock
	h.mu.Unlock()
}

func (h *Handler) getListeningSock() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.listeningSock
}

// ------------------------------------------------
//
// ------------------------------------------------

func (h *Handler) InitServer(server models.Server) {

	var wg sync.WaitGroup
	for range server.InitialPoolLen() {
		wg.Go(func() {
			sock := h.createSock()
			h.addTimeoutForSock(sock, server.GetTimeout())
			_ = h.connectServerSock(sock, server) // при ошибке в инициализации просто упадем
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

func (h *Handler) connectServerSock(sock int, server models.Server) error {
	addr := server.GetAddrIp4()
	
	retriesAvailable := server.GetRetries()
	for {
		if retriesAvailable == 0 {
			return models.ErrRetriesFailed
		}
		err := h.sys.Connect(sock, &addr)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINPROGRESS) {
				err = h.poll(sock, "connect")
				if err != nil {
					if err == models.ErrPollTimeout {
						retriesAvailable--

						continue
					}
					if errors.Is(err, syscall.ECONNREFUSED) {
						retriesAvailable--

						continue
					}

					return err
				}

				break
			}
			
			return err
		}

		break
	}
	return nil
}

func (h *Handler) poll(sock int, eventType string) error {
	pUnit := models.PollingUnit{
		SocketFd: sock,
		EventType: eventType,
		ResultChan: make(chan error),
	}

	err := h.poller.Add(pUnit)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(h.getTimeoutForSock(sock))
	for {
		select {
		case err = <-pUnit.ResultChan:
			if err != nil {
				return err
			}

		default:
			if time.Now().After(deadline) {
				h.poller.DeleteSocketFromPolling(sock)

				return models.ErrPollTimeout
			}
			runtime.Gosched()

			continue
		}

		break
	}

	return nil
}

func (h *Handler) pollWithoutTimeout(sock int, eventType string) error {
	pUnit := models.PollingUnit{
		SocketFd: sock,
		EventType: eventType,
		ResultChan: make(chan error),
	}

	err := h.poller.Add(pUnit)
	if err != nil {
		return err
	}

	select {
	case err = <-pUnit.ResultChan:
		if err != nil {
			return err
		}

	case <-h.cancelCh:

	}

	return nil
}


// ------------------------------------------------
//
// ------------------------------------------------


func (h *Handler) Listen(addr models.Address) {
	s := h.createSock()

	// потенциальная проблема с переиспользованием порта после перезапуска
	// + возможность запустить несколько слушателей

	err := h.sys.Bind(s, &syscall.SockaddrInet4{
		Port: addr.Port,
		Addr: addr.IP,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = h.sys.Listen(s, 10) // 10 - очередь ожидающих клиентов; дать возможность настройки в конфиге?
	if err != nil {
		log.Fatal(err)
	}

	h.setListeningSock(s)
}

func (h *Handler) Accept() *models.Client {
	for {
		clientSock, clientAddrRaw, err := h.sys.Accept(h.getListeningSock())
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				err = h.pollWithoutTimeout(h.listeningSock, "income")
				if err != nil {
					log.Fatal(err)
				}

				continue
			}
			log.Fatal(err)
		}

		// проверить на реальном сисколле
		addr, ok := clientAddrRaw.(*syscall.SockaddrInet4)
		if !ok {
			log.Fatal("Ошибка утверждения Sockaddr")
		}

		return &models.Client{
			Sock: clientSock,
			Addr: models.Address{
				IP: addr.Addr,
				Port: addr.Port,
			},
		}
	}
}

func (h *Handler) StopAccepting() {
	close(h.cancelCh)
}

// Close закрывает клиентский сокет без соединения с сервером,
// т.к. лимит клиентов превышен
func (h *Handler) Close(client *models.Client) {
	h.sys.CloseSocket(client.Sock)
}

// TCPProxy выполняет проксирование сообщений между client и server
func (h *Handler) TCPProxy(client *models.Client, server models.Server) error {
	serverSock := h.getServerSockFromPool(server.GetID())
	// В пуле нет сокетов
	if serverSock == 0 {
		// Создаем новое подключение
		serverSock = h.createSock()
		err := h.connectServerSock(serverSock, server)
		if err != nil {
			return err
		}
	}
	
	clientUnit := models.PollingUnit{
		SocketFd: client.Sock,
		EventType: "income",
		ResultChan: make(chan error),
	}
	serverUnit := models.PollingUnit{
		SocketFd: serverSock,
		EventType: "income",
		ResultChan: make(chan error),
	}

	retriesAvailable := server.GetRetries()
	for {
		h.poller.Add(clientUnit)
		h.poller.Add(serverUnit)

		responseDeadline := time.Now().Add(server.GetTimeout())
		idleDeadline := time.Now().Add(server.GetIdleTimeout())

		if retriesAvailable == 0 {
			return models.ErrRetriesFailed // надо бы понимать, кто не ответил вовремя
		}

		// ждем события
		select {

		// пришло сообщение от клиента
		case <-clientUnit.ResultChan:
			// копируем данные из клиентского сокета в серверный
			h.sys.Splice()

			// клиент может закрыть соединение

		// пришло сообщение от сервера
		case <-serverUnit.ResultChan:
			// копируем данные из серверного сокета в клиентский
			h.sys.Splice()

			// сервер может закрыть соединение

		default:
			if time.Now().After(responseDeadline) {
				retriesAvailable--

			}
			if time.Now().After(idleDeadline) {
				// проверяем, можно ли положить серверный сокет в пул
				
				return nil
			}
			runtime.Gosched()

			continue
		}
	}
}

type serverSock struct {
	status string // ready, running
	sock int
	lastActivity time.Time
}