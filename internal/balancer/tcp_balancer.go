package balancer

import (
	"log"
	"math/rand"
	"sync"

	"github.com/rseleznev/bazik/internal/models"
)

type networker interface {
	NewTCPListener(models.Address) (models.Listener, error)
	NewConn(models.Address) (models.Conn, error)
}

type TCPBalancer struct {
	opts *models.BalancerOptions
	mu sync.RWMutex
	servers []*server
	chats map[string]*chat

	net networker
}

func (b *TCPBalancer) Run() {
	listener, err := b.net.NewTCPListener(b.opts.Addr)
	if err != nil {
		log.Fatal(err)
	}
	// таймеры и настройки (TCP_NODELAY) слушающего сокета

	for {
		newClient, _ := listener.Accept()
		go b.link(newClient)
	}
}

func (b *TCPBalancer) link(c models.Conn) {
	b.mu.RLock()
	if len(b.chats) >= b.opts.MaxClientsAmount {
		b.mu.RUnlock()
		c.Close()

		return
	}
	b.mu.RUnlock()

	var server *server
	var s models.Conn
	var err error
	
	for {
		server = b.findServer()
		s, err = server.getConn()
		if err != nil {
			// у конкретного сервера нет свободных соединений
			if err == models.ErrNoConnsAvailable {
				// логируем ошибку
				continue
			}
			
			// логируем ошибку без Fatal
			c.Close()
			return
		}
		break
	}
	id := c.GetRawAddr() + " / " + s.GetRawAddr()

	chat := &chat{
		id: id,
		mu: sync.RWMutex{},
		mainTimeout: server.getMainTimeout(),
		idleTimeout: server.getIdleTimeout(),

		client: c,
		server: s,
	}
	b.mu.Lock()
	b.chats[chat.id] = chat
	b.mu.Unlock()
	b.process(chat)
}

func (b *TCPBalancer) process(c *chat) {
	availableRetries := b.opts.RetryAmount
	for {
		err := c.tcpProxy()
		if err != nil {
			if err == models.ErrClientSide {
				// в случае клиентской ошибки просто прекращаем работу,
				// ждем новое соединение от клиента
				c.close()
				b.mu.Lock()
				delete(b.chats, c.id)
				b.mu.Unlock()
				return
			}
			if availableRetries > 0 { // проверяем, разрешены ли ретраи
				// пробуем найти новый сервер
				availableRetries--
				tryCounter := 1 // защита от бесконечного цикла поиска сервера
				for {
					if tryCounter > 3 { // слишком много неудачных попыток найти новый сервер
						c.close()
						b.mu.Lock()
						delete(b.chats, c.id)
						b.mu.Unlock()
						return
					}
					newServer, err := b.findServer().getConn()
					if err != nil {
						if err == models.ErrNoConnsAvailable {
							// логируем ошибку
							tryCounter++
							continue
						}
						
						// логируем ошибку без Fatal
						c.close()
						b.mu.Lock()
						delete(b.chats, c.id)
						b.mu.Unlock()
						return
					}
					c.server = newServer
					break
				}
				
				continue
			}
			// если ретраи не разрешены, закрываем соединение с клиентом
			c.close()
			b.mu.Lock()
			delete(b.chats, c.id)
			b.mu.Unlock()
			return
		}
		// если чат завершился без ошибок,
		// т.е. по таймеру бездействия
		break
	}
	b.mu.Lock()
	delete(b.chats, c.id)
	b.mu.Unlock()
	b.storeConn(c.server)
}

func (b *TCPBalancer) findServer() *server {
	switch b.opts.BalancerAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n]

	default:
		return b.servers[0]

	}
}

func (b *TCPBalancer) storeConn(c models.Conn) {
	addr := c.GetRawAddr()
	for _, s := range b.servers {
		if s.opts.Addr.Raw == addr {
			s.storeConn(c)
			break
		}
	}
}