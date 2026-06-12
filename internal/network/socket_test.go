package network

import (
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

var testSocket = &socket{
	fd: 1,
	mu: sync.RWMutex{},
	addr: models.Address{
		IP: [4]byte{127, 0, 0, 1},
		Port: 3000,
	},
	timeout: time.Millisecond*500,
}

type mockSys struct{
	acceptFunc func(int) (int, syscall.Sockaddr, error)
	newSocketFunc func(int, int, int) (int, error)
	closeFunc func(int) error
	bindFunc func(int, syscall.Sockaddr) error
	listenFunc func(int, int) error
	connectFunc func(int, syscall.Sockaddr) error
	spliceFunc func(writer, reader int) (int64, error)
	pipeFunc func() (int, int, error)
	getUnreadFunc func(int) (int, error)
	getUnsentFunc func(int) (int, error)
}
func (m mockSys) Accept(n int) (int, syscall.Sockaddr, error) {
	return m.acceptFunc(n)
}
func (m mockSys) NewSocket(d int, t int, p int) (int, error) {
	return m.newSocketFunc(d, t, p)
}
func (m mockSys) Close(n int) error {
	return m.closeFunc(n)
}
func (m mockSys) Bind(n int, a syscall.Sockaddr) error {
	return m.bindFunc(n, a)
}
func (m mockSys) Listen(n int, q int) error {
	return m.listenFunc(n, q)
}
func (m mockSys) Connect(n int, a syscall.Sockaddr) error {
	return m.connectFunc(n, a)
}
func (m mockSys) Splice(w, r int) (int64, error) {
	return m.spliceFunc(w, r)
}
func (m mockSys) Pipe() (int, int, error) {
	return m.pipeFunc()
}
func (m mockSys) GetUnread(n int) (int, error) {
	return m.getUnreadFunc(n)
}
func (m mockSys) GetUnsent(n int) (int, error) {
	return m.getUnsentFunc(n)
}

type mockPoller struct{
	addFunc func(models.PollingUnit) error
	stopUnitPollingFunc func(u models.PollingUnit)
}
func (p mockPoller) Add(u models.PollingUnit) error {
	return p.addFunc(u)
}
func (p mockPoller) StopUnitPolling(u models.PollingUnit) {
	p.stopUnitPollingFunc(u)
}

func TestAccept(t *testing.T) {
	requestCounter := 0
	
	testCases := []struct{
		name string
		expectedErr error
		sys mockSys
		p mockPoller
	}{
		{
			name: "success simple",
			expectedErr: nil,
			sys: mockSys{
				acceptFunc: func(_ int) (int, syscall.Sockaddr, error) {
					return 3, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}, Port: 7000}, nil
				},
			},
		},
		{
			name: "success with polling",
			expectedErr: nil,
			sys: mockSys{
				acceptFunc: func(_ int) (int, syscall.Sockaddr, error) {
					if requestCounter == 0 {
						requestCounter++
						return 0, nil, syscall.EWOULDBLOCK	
					}
					return 3, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}, Port: 7000}, nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func ()  {
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testSocket.sys = tc.sys
			testSocket.poller = tc.p
			
			_, err := testSocket.Accept()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
			requestCounter = 0
		})
	}
}

func TestConnect(t *testing.T) {
	requestCounter := 0
	
	testCases := []struct{
		name string
		expectedErr error
		addr models.Address
		sys mockSys
		p mockPoller
	}{
		{
			name: "success simple",
			expectedErr: nil,
			addr: models.Address{
				IP: [4]byte{127, 0, 0, 1},
				Port: 3000,
			},
			sys: mockSys{
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					return nil
				},
			},
		},
		{
			name: "success with polling",
			expectedErr: nil,
			addr: models.Address{
				IP: [4]byte{127, 0, 0, 1},
				Port: 3000,
			},
			sys: mockSys{
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					if requestCounter == 0 {
						requestCounter++
						return syscall.EINPROGRESS
					}
					return nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func ()  {
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "fail ErrTimeout",
			expectedErr: models.ErrTimeout,
			addr: models.Address{
				IP: [4]byte{127, 0, 0, 1},
				Port: 3000,
			},
			sys: mockSys{
				connectFunc: func(_ int, _ syscall.Sockaddr) error {
					if requestCounter == 0 {
						requestCounter++
						return syscall.EINPROGRESS
					}
					return nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func ()  {
						time.Sleep(time.Millisecond*600)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
				stopUnitPollingFunc: func(_ models.PollingUnit) {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testSocket.sys = tc.sys
			testSocket.poller = tc.p

			err := testSocket.Connect()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
			requestCounter = 0
		})
	}
}

func TestCopyTo(t *testing.T) {
	requestCounter := 0
	
	testCases := []struct{
		name string
		dst *socket
		expectedErr error
		sys mockSys
		p mockPoller
	}{
		{
			name: "success",
			dst: &socket{
				fd: 2,
				mu: sync.RWMutex{},
				addr: models.Address{
					IP: [4]byte{127, 0, 0, 1},
					Port: 7000,
				},
			},
			expectedErr: nil,
			sys: mockSys{
				closeFunc: func(_ int) error {
					return nil
				},
				pipeFunc: func() (int, int, error) {
					return 10, 11, nil
				},
				spliceFunc: func(_, _ int) (int64, error) {
					return 0, nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func() {
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "success with polling",
			dst: &socket{
				fd: 2,
				mu: sync.RWMutex{},
				addr: models.Address{
					IP: [4]byte{127, 0, 0, 1},
					Port: 7000,
				},
			},
			expectedErr: nil,
			sys: mockSys{
				closeFunc: func(_ int) error {
					return nil
				},
				pipeFunc: func() (int, int, error) {
					return 10, 11, nil
				},
				spliceFunc: func(_, _ int) (int64, error) {
					if requestCounter == 0 {
						requestCounter++
						return 0, syscall.EAGAIN
					}
					if requestCounter == 1 {
						requestCounter++
						return 0, nil
					}
					if requestCounter == 3 {
						requestCounter++
						return 0, syscall.EAGAIN
					}
					return 0, nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func() {
						time.Sleep(time.Millisecond*100)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
			},
		},
		{
			name: "fail ErrIdleTimeout",
			dst: &socket{
				fd: 2,
				mu: sync.RWMutex{},
				addr: models.Address{
					IP: [4]byte{127, 0, 0, 1},
					Port: 7000,
				},
			},
			expectedErr: models.ErrIdleTimeout,
			sys: mockSys{
				closeFunc: func(_ int) error {
					return nil
				},
				pipeFunc: func() (int, int, error) {
					return 10, 11, nil
				},
				spliceFunc: func(_, _ int) (int64, error) {
					return 0, nil
				},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func() {
						time.Sleep(time.Second*2)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
				stopUnitPollingFunc: func(_ models.PollingUnit) {},
			},
		},
		{
			name: "fail ErrTimeout",
			dst: &socket{
				fd: 2,
				mu: sync.RWMutex{},
				addr: models.Address{
					IP: [4]byte{127, 0, 0, 1},
					Port: 7000,
				},
			},
			expectedErr: models.ErrTimeout,
			sys: mockSys{
				closeFunc: func(_ int) error {
					return nil
				},
				pipeFunc: func() (int, int, error) {
					return 10, 11, nil
				},
				spliceFunc: func(_, _ int) (int64, error) {
					return 0, syscall.EAGAIN
				},
				getUnreadFunc: func(i int) (int, error) {return 10, nil},
			},
			p: mockPoller{
				addFunc: func(pu models.PollingUnit) error {
					go func() {
						time.Sleep(time.Millisecond*700)
						pu.ResultChan <- nil
					}()
					
					return nil
				},
				stopUnitPollingFunc: func(_ models.PollingUnit) {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testSocket.sys = tc.sys
			testSocket.poller = tc.p
			testSocket.idleDeadline = time.Now().Add(time.Second*1)

			err := testSocket.CopyTo(tc.dst)
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
			requestCounter = 0
		})
	}
}