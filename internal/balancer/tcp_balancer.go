package balancer

import (
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type networker interface {
	NewTCPListener(models.Address) (models.Listener, error)
	NewTCPConn(models.Address) (models.Conn, error)
}

type TCPBalancer struct {
	opts *models.BalancerOptions
	mu sync.RWMutex
	servers []*server
	chats map[string]*chat

	net networker
}

func (b *TCPBalancer) Run() {
	slog.Info("запуск TCPBalancer", "module", "tcp_balancer")
	listener, err := b.net.NewTCPListener(b.opts.Addr)
	if err != nil {
		slog.Error("ошибка создания слушающего сокета", "module", "tcp_balancer", "localAddr", b.opts.Addr.Raw, "err", err)
		os.Exit(1)
	}
	// таймеры и настройки (TCP_NODELAY) слушающего сокета

	slog.Info("запуск слушающего сокета", "module", "tcp_balancer")
	for {
		newClient, _ := listener.Accept()
		go b.link(newClient)
	}
}

func (b *TCPBalancer) link(c models.Conn) {
	slog.Info("создание связи клиент-сервер", "module", "tcp_balancer")
	b.mu.RLock()
	if len(b.chats) >= b.getMaxClientAmount() {
		b.mu.RUnlock()
		c.Close()
		slog.Info("достигнут лимит MaxClientsAmount, соединение с клиентом закрыто", "module", "tcp_balancer")

		return
	}
	b.mu.RUnlock()

	var server *server
	var s models.Conn
	var err error
	
	slog.Info("подбор сервера", "module", "tcp_balancer")
	for {
		server = b.findServer()
		s, err = server.getConn()
		if err != nil {
			// у конкретного сервера нет свободных соединений
			if err == models.ErrNoConnsAvailable {
				slog.Warn("у сервера нет свободных соединений", "module", "tcp_balancer", "serverAddr", server.opts.Addr.Raw)
				continue
			}
			
			slog.Warn("ошибка сервера", "module", "tcp_balancer", "serverAddr", server.opts.Addr.Raw, "err", err)
			c.Close()
			return
		}
		break
	}
	id := c.GetRawAddr() + " / " + s.GetRawAddr()
	slog.Info("сервер подобран", "module", "tcp_balancer")

	chat := &chat{
		id: id,
		mu: sync.Mutex{},
		mainTimeout: b.getMainTimeout(),
		idleTimeout: server.getIdleTimeout(),

		client: c,
		server: s,
	}
	slog.Info("создан чат", "module", "tcp_balancer", "chatId", chat.id)
	b.addChat(chat)
	b.process(chat)
}

func (b *TCPBalancer) process(c *chat) {
	slog.Info("процессинг чата", "module", "tcp_balancer", "chatId", c.id)
	availableRetries := b.getRetryAmount()
	for {
		err := c.tcpProxy()
		if err != nil {
			if err == models.ErrClientSide {
				// в случае клиентской ошибки просто прекращаем работу,
				// ждем новое соединение от клиента
				c.client.Close()
				b.closeServerConn(c.server)
				b.deleteChat(c)
				slog.Warn("ошибка на клиентской стороне", "module", "tcp_balancer", "chatId", c.id)
				slog.Info("остановка чата", "module", "tcp_balancer", "chatId", c.id)

				// разобраться, почему здесь соединение не получает сообщение клиента
				return
			}
			slog.Warn("ошибка на серверной стороне", "module", "tcp_balancer", "chatId", c.id)
			if availableRetries > 0 { // проверяем, разрешены ли ретраи
				// пробуем найти новый сервер
				slog.Info("попытка найти новый сервер", "module", "tcp_balancer", "chatId", c.id)
				availableRetries--
				tryCounter := 1 // защита от бесконечного цикла поиска сервера
				for {
					if tryCounter > 3 { // слишком много неудачных попыток найти новый сервер
						c.client.Close()
						b.closeServerConn(c.server)
						b.deleteChat(c)
						slog.Warn("слишком много попыток найти новый сервер", "module", "tcp_balancer", "chatId", c.id)
						slog.Info("остановка чата", "module", "tcp_balancer", "chatId", c.id)
						return
					}
					newServer, err := b.findServer().getConn()
					if err != nil {
						if err == models.ErrNoConnsAvailable {
							slog.Warn("у сервера нет свободных соединений", "module", "tcp_balancer", "serverAddr", newServer.GetRawAddr())
							tryCounter++
							continue
						}
						
						c.client.Close()
						b.closeServerConn(c.server)
						b.deleteChat(c)
						slog.Warn("ошибка сервера", "module", "tcp_balancer", "serverAddr", newServer.GetRawAddr(), "err", err)
						slog.Info("остановка чата", "module", "tcp_balancer", "chatId", c.id)
						return
					}
					c.server = newServer
					break
				}
				slog.Info("новый сервер подобран", "module", "tcp_balancer", "chatId", c.id)
				
				continue
			}
			// если ретраи не разрешены, закрываем соединение с клиентом
			c.close()
			b.deleteChat(c)
			slog.Info("остановка чата без ретраев", "module", "tcp_balancer", "chatId", c.id)
			return
		}
		// если чат завершился без ошибок,
		// т.е. по таймеру бездействия
		slog.Info("остановка чата по таймеру бездействия", "module", "tcp_balancer", "chatId", c.id)
		break
	}
	c.client.Close()
	b.deleteChat(c)
	b.storeServerConn(c.server)
}

func (b *TCPBalancer) findServer() *server {
	switch b.opts.BalancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n]

	default:
		return b.servers[0]

	}
}

func (b *TCPBalancer) storeServerConn(c models.Conn) {
	addr := c.GetRawAddr()
	for _, s := range b.servers {
		if s.opts.Addr.Raw == addr {
			s.storeConn(c)
			break
		}
	}
}

func (b *TCPBalancer) closeServerConn(c models.Conn) {
	addr := c.GetRawAddr()
	for _, s := range b.servers {
		if s.opts.Addr.Raw == addr {
			s.closeConn(c)
			break
		}
	}
}

func (b *TCPBalancer) addChat(c *chat) {
	b.mu.Lock()
	b.chats[c.id] = c
	b.mu.Unlock()
}

func (b *TCPBalancer) deleteChat(c *chat) {
	b.mu.Lock()
	delete(b.chats, c.id)
	b.mu.Unlock()
}

func (b *TCPBalancer) getMaxClientAmount() int {
	return b.opts.MaxClientsAmount
}

func (b *TCPBalancer) getMainTimeout() time.Duration {
	return time.Duration(b.opts.MainTimeout)*time.Millisecond
}

func (b *TCPBalancer) getRetryAmount() int {
	return b.opts.RetryAmount
}