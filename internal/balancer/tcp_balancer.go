package balancer

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type networker interface {
	NewTCPListener() (conn, error)
	NewConn(models.Address) (conn, error)
}

type conn interface {
	Connect() error
	Accept() conn
	Close()
	CopyTo(conn) error
	SetIdleDeadline(time.Time)
	LogActivity()
	LastActivity() time.Time
	SetLastActivity(time.Time)
	GetRawAddr() string
}

type TCPBalancer struct {
	opts *options
	mu sync.RWMutex
	servers []*server
	chats map[string]*chat

	net networker
}

func (b *TCPBalancer) run() {
	// создаем серверы
	
	for _, s := range b.servers {
		err := s.init()
		if err != nil {
			log.Fatal(err)
		}
	}
	
	listener, err := b.net.NewTCPListener()
	if err != nil {
		log.Fatal(err)
	}
	// таймеры и настройки (TCP_NODELAY) слушающего сокета

	for {
		newClient := listener.Accept()
		go b.link(newClient)
	}
}

func (b *TCPBalancer) link(c conn) {
	b.mu.RLock()
	if len(b.chats) >= b.opts.MaxClientsAmount {
		b.mu.RUnlock()
		c.Close()

		return
	}
	b.mu.RUnlock()

	var s conn
	var err error
	
	for {
		s, err = b.findServer().getConn()
		if err != nil {
			if err == models.ErrNoConnsAvailable {
				// логируем ошибку
				continue
			}
			
			// логируем ошибку без Fatal
			return
		}
		break
	}

	chat := &chat{
		id: "client+server addr hash?",
		client: c,
		server: s,
	}
	b.mu.Lock()
	b.chats[chat.id] = chat
	b.mu.Unlock()
	b.process(chat)
}

func (b *TCPBalancer) process(c *chat) {
	for {
		canRetry, err := c.tcpProxy()
		if err != nil {
			if canRetry {
				for {
					newServer, err := b.findServer().getConn()
					if err != nil {
						if err == models.ErrNoConnsAvailable {
							// логируем ошибку
							continue
						}
						
						// логируем ошибку без Fatal
						return
					}
					c.server = newServer
					break
				}
				
				continue
			}
			c.close()
		}
		break
	}
	b.mu.Lock()
	delete(b.chats, c.id)
	b.mu.Unlock()
	b.storeConn(c.server)
}

func (b *TCPBalancer) findServer() *server {
	switch b.opts.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n]

	default:
		return b.servers[0]

	}
}

func (b *TCPBalancer) storeConn(c conn) {
	addr := c.GetRawAddr()
	for _, s := range b.servers {
		if s.addr.Raw == addr {
			s.storeConn(c)
			break
		}
	}
}