package balancer

import (
	"log"
	"math/rand"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type networker interface {
	NewTCPListener() (conn, error)
	NewConn(models.Address) (conn, error)
}

type conn interface {
	Accept() conn
	Close()
	CopyTo(conn) error
	SetIdleDeadline(time.Time)
	LogActivity()
	LastActivity() time.Time
	SetLastActivity(time.Time)
}

type TCPBalancer struct {
	opts *options
	servers []*server
	chats map[string]*chat

	net networker
}

func (b *TCPBalancer) run() {
	listener, err := b.net.NewTCPListener()
	if err != nil {
		log.Fatal(err)
	}

	for {
		newClient := listener.Accept()
		go b.link(newClient)
	}
}

func (b *TCPBalancer) link(c conn) {
	// проверяем, можем ли принять клиента
	
	s := b.findServer()

	chat := &chat{
		id: "client+server addr hash?",
		client: c,
		server: s,
	}
	b.chats[chat.id] = chat
	b.process(chat)
}

func (b *TCPBalancer) findServer() conn {
	switch b.opts.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n].connPool[0]

	default:
		return b.servers[0].connPool[0]

	}
}

func (b *TCPBalancer) process(c *chat) {
	for {
		canRetry, err := c.tcpProxy()
		if err != nil {
			if canRetry {
				newServer := b.findServer()
				c.server = newServer
				continue
			}
			c.close()
		}
		break
	}
	delete(b.chats, c.id)
	// возвращаем серверный сокет в буфер
	// как имея сокет найти сервер?
}