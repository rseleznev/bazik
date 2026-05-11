package handler

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

var testHandler = &Handler{
	mu: sync.RWMutex{},
	serverSocksPool: make(map[string][]int),
	socksTimeout: make(map[int]time.Duration),
}

type mockSyscalls struct {
	newSocketFunc func(int, int, int) (int, error)
	closeSocketFunc func(int) error
	bindFunc func(int, syscall.Sockaddr) error
	listenFunc func(int, int) error
	acceptFunc func(int) (int, syscall.Sockaddr, error)
	connectFunc func(int, syscall.Sockaddr) error
	spliceFunc func()
}
func (ms mockSyscalls) NewSocket(d int, t int, p int) (int, error) {
	return ms.newSocketFunc(d, t, p)
}
func (ms mockSyscalls) CloseSocket(s int) error {
	return ms.closeSocketFunc(s)
}
func (ms mockSyscalls) Bind(s int, a syscall.Sockaddr) error {
	return ms.bindFunc(s, a)
}
func (ms mockSyscalls) Listen(s int, q int) error {
	return ms.listenFunc(s, q)
}
func (ms mockSyscalls) Accept(s int) (int, syscall.Sockaddr, error) {
	return ms.acceptFunc(s)
}
func (ms mockSyscalls) Connect(s int, a syscall.Sockaddr) error {
	return ms.connectFunc(s, a)
}
func (ms mockSyscalls) Splice() {

}

type mockPoller struct {
	addFunc func(models.PollingUnit) error
}
func (mp mockPoller) Add(pu models.PollingUnit) error {
	return mp.addFunc(pu)
}
func (mp mockPoller) DeleteSocketFromPolling(_ int) {
}

type mockServer struct {
	getAddrIp4Func func() syscall.SockaddrInet4
	initialPoolLenFunc func() int
	maxPoolLenFunc func() int
	getIDFunc func() string
	getTimeoutFunc func() time.Duration
	getRetriesFunc func() int
}
func (ms mockServer) GetAddrIp4() syscall.SockaddrInet4 {
	return ms.getAddrIp4Func()
}
func (ms mockServer) InitialPoolLen() int {
	return ms.initialPoolLenFunc()
}
func (ms mockServer) MaxPoolLen() int {
	return ms.maxPoolLenFunc()
}
func (ms mockServer) GetID() string {
	return ms.getIDFunc()
}
func (ms mockServer) GetTimeout() time.Duration {
	return ms.getTimeoutFunc()
}
func (ms mockServer) GetRetries() int {
	return ms.getRetriesFunc()
}


func TestInitServer(t *testing.T) {
	var iterator atomic.Int64
	testData := []struct{
		name string
		servers []mockServer
		sys mockSyscalls
		p mockPoller
	}{
		{
			name: "success 1 server 1 sock",
			servers: []mockServer{
				{
					initialPoolLenFunc: func() int {return 1},
					getTimeoutFunc: func() time.Duration {return time.Second*3},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 3},
					getIDFunc: func() string {return "testInitServerId1"},
				},
			},
			sys: mockSyscalls{
				newSocketFunc: func(_, _, _ int) (int, error) {
					return int(iterator.Add(1)), nil
				},
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return fmt.Errorf("test err: %w", syscall.EAGAIN)
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func () {
						time.Sleep(time.Second)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "success 1 server few socks",
			servers: []mockServer{
				{
					initialPoolLenFunc: func() int {return rand.Intn(7)},
					getTimeoutFunc: func() time.Duration {return time.Second*3},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 3},
					getIDFunc: func() string {return "testInitServerId2"},
				},
			},
			sys: mockSyscalls{
				newSocketFunc: func(_, _, _ int) (int, error) {
					return int(iterator.Add(1)), nil
				},
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return fmt.Errorf("test err: %w", syscall.EAGAIN)
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func () {
						time.Sleep(time.Second)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "success few servers 1 sock",
			servers: []mockServer{
				{
					initialPoolLenFunc: func() int {return 1},
					getTimeoutFunc: func() time.Duration {return time.Second*3},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 3},
					getIDFunc: func() string {return "testInitServerId3"},
				},
				{
					initialPoolLenFunc: func() int {return 1},
					getTimeoutFunc: func() time.Duration {return time.Second*2},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 2},
					getIDFunc: func() string {return "testInitServerId4"},
				},
			},
			sys: mockSyscalls{
				newSocketFunc: func(_, _, _ int) (int, error) {
					return int(iterator.Add(1)), nil
				},
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return fmt.Errorf("test err: %w", syscall.EAGAIN)
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func () {
						time.Sleep(time.Second)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "success few servers few socks",
			servers: []mockServer{
				{
					initialPoolLenFunc: func() int {return rand.Intn(3)},
					getTimeoutFunc: func() time.Duration {return time.Second*3},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 3},
					getIDFunc: func() string {return "testInitServerId5"},
				},
				{
					initialPoolLenFunc: func() int {return 2},
					getTimeoutFunc: func() time.Duration {return time.Second*2},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 2},
					getIDFunc: func() string {return "testInitServerId6"},
				},
				{
					initialPoolLenFunc: func() int {return rand.Intn(7)},
					getTimeoutFunc: func() time.Duration {return time.Second*5},
					getAddrIp4Func: func() syscall.SockaddrInet4 {
						return syscall.SockaddrInet4{
							Port: 5000,
							Addr: [4]byte{127, 0, 0, 1},
						}
					},
					getRetriesFunc: func() int {return 5},
					getIDFunc: func() string {return "testInitServerId7"},
				},
			},
			sys: mockSyscalls{
				newSocketFunc: func(_, _, _ int) (int, error) {
					return int(iterator.Add(1)), nil
				},
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return fmt.Errorf("test err: %w", syscall.EAGAIN)
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func () {
						time.Sleep(time.Second)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			testHandler.sys = tt.sys
			testHandler.poller = tt.p

			for _, s := range tt.servers {
				testHandler.InitServer(s)
			}
			t.Log("Таймауты сокетов: ", testHandler.socksTimeout)
			t.Log("Пул сокетов: ", testHandler.serverSocksPool)
		})
	}
}

func Test_connectServerSock(t *testing.T) {
	var requestCounter int
	testData := []struct{
		name string
		expectedErr error
		server mockServer
		sys mockSyscalls
		p mockPoller
	}{
		{
			name: "success with retry",
			expectedErr: nil,
			server: mockServer{
				getAddrIp4Func: func() syscall.SockaddrInet4 {
					return syscall.SockaddrInet4{
						Port: 5000,
						Addr: [4]byte{127, 0, 0, 1},
					}
				},
				getRetriesFunc: func() int {return 3},
			},
			sys: mockSyscalls{
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					if requestCounter == 0 {
						requestCounter++
						return fmt.Errorf("test err: %w", syscall.EAGAIN)
					}
					return nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {					
					return nil
				},
			},
		},
		{
			name: "fail ErrRetriesFailed",
			expectedErr: models.ErrRetriesFailed,
			server: mockServer{
				getAddrIp4Func: func() syscall.SockaddrInet4 {
					return syscall.SockaddrInet4{
						Port: 5000,
						Addr: [4]byte{127, 0, 0, 1},
					}
				},
				getRetriesFunc: func() int {return 3},
			},
			sys: mockSyscalls{
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return fmt.Errorf("test err: %w", syscall.EAGAIN)
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {					
					return nil
				},
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			testHandler.sys = tt.sys
			testHandler.poller = tt.p
			testHandler.addTimeoutForSock(125, time.Millisecond*100)

			err := testHandler.connectServerSock(125, tt.server)
			if err != tt.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено : %s", tt.expectedErr, err)
			}
		})
	}
}