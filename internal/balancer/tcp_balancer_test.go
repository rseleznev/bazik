package balancer

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

var testBalancer *TCPBalancer

type mockNetworker struct {
	newTCPListenerFunc func(models.Address) (models.Listener, error)
	newTCPConnFunc func(models.Address) (models.Conn, error)
}
func (m mockNetworker) NewTCPListener(a models.Address) (models.Listener, error) {
	return m.newTCPListenerFunc(a)
}
func (m mockNetworker) NewTCPConn(a models.Address) (models.Conn, error) {
	return m.newTCPConnFunc(a)
}

type mockConn struct {
	withTimerFunc func(*time.Timer)
	withCancelFunc func(ch chan struct{})
	cancelFunc func()
	connectFunc func() error
	copyToFunc func(models.Conn) error
	closeFunc func()
	setIdleTimeoutFunc func(time.Duration)
	getRawAddrFunc func() string
	checkUnreadFunc func() (int, error)
	checkUnsentFunc func() (int, error)
	getFdFunc func() int
	setMainTimeoutFunc func(time.Duration)
}
func (m mockConn) WithTimer(t *time.Timer) {
	m.withTimerFunc(t)
}
func (m mockConn) WithCancel(ch chan struct{}) {
	m.withCancelFunc(ch)
}
func (m mockConn) Cancel() {
	m.cancelFunc()
}
func (m mockConn) Connect() error {
	return m.connectFunc()
}
func (m mockConn) CopyTo(dst models.Conn) error {
	return m.copyToFunc(dst)
}
func (m mockConn) Close() {
	m.closeFunc()
}
func (m mockConn) SetIdleTimeout(time.Duration) {}
func (m mockConn) GetRawAddr() string {
	return m.getRawAddrFunc()
}
func (m mockConn) CheckUnread() (int, error) {
	return m.checkUnreadFunc()
}
func (m mockConn) CheckUnsent() (int, error) {
	return m.checkUnsentFunc()
}
func (m mockConn) GetFd() int {
	return m.getFdFunc()
}
func (m mockConn) SetMainTimeout(t time.Duration) {}


func TestMain(m *testing.M) {
	servers := []*server{
		{
			opts: &models.ServerOptions{
				Addr: models.Address{
					Raw: "127.0.0.1:7000",
				},

				MaxClientsAmount: 1,
				MaxIdleSeconds: 300,
				MaxConnsPoolLen: 5,
				InitialConnsPoolLen: 2,
			},
			connPool: make(chan models.Conn, 5),
		},
		{
			opts: &models.ServerOptions{
				Addr: models.Address{
					Raw: "127.0.0.1:8000",
				},

				MaxClientsAmount: 1,
				MaxIdleSeconds: 300,
				MaxConnsPoolLen: 5,
				InitialConnsPoolLen: 2,
			},
			connPool: make(chan models.Conn, 5),
		},
	}

	for i, v := range servers {
		for range v.opts.InitialConnsPoolLen {
			servers[i].connPool <- mockConn{
				getFdFunc: func() int {return 5},
				getRawAddrFunc: func() string {
					return v.opts.Addr.Raw
				},
				copyToFunc: func(_ models.Conn) error {
					return models.ErrIdleTimeout
				},
				checkUnreadFunc: func() (int, error) {
					return 0, nil
				},
				checkUnsentFunc: func() (int, error) {
					return 0, nil
				},
			}
		}
	}
	
	testBalancer = &TCPBalancer{
		opts: &models.BalancerOptions{
			BalancingAlg: "random",

			MainTimeout: int(time.Millisecond)*500,
			MaxClientsAmount: 1,
		},
		mu: sync.RWMutex{},
		servers: servers,
		chats: make(map[string]*chat, 5),
	}

	code := m.Run()
	os.Exit(code)
}

func Test_link(t *testing.T) {
	testCases := []struct{
		name string
		clientConn models.Conn
	}{
		{
			name: "success",
			clientConn: mockConn{
				getFdFunc: func() int {return 3},
				getRawAddrFunc: func() string {
					return "127.0.0.1:3000"
				},
				copyToFunc: func(_ models.Conn) error {
					return models.ErrIdleTimeout
				},
				closeFunc: func() {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testBalancer.link(tc.clientConn)
		})
	}
}