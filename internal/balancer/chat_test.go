package balancer

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

func Test_tcpProxy(t *testing.T) {
	requestCounter := 0
	
	testCases := []struct{
		name string
		expectedErr error
		client mockConn
		server mockConn
	}{
		{
			name: "success fast",
			expectedErr: nil,
			client: mockConn{
				getFdFunc: func() int {return 3},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {return models.ErrIdleTimeout},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
			server: mockConn{
				getFdFunc: func() int {return 5},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {return models.ErrIdleTimeout},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
		},
		{
			name: "success with client activity",
			expectedErr: nil,
			client: mockConn{
				getFdFunc: func() int {return 3},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter == 0 {
						requestCounter++
						return nil
					}
					return models.ErrIdleTimeout
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
			server: mockConn{
				getFdFunc: func() int {return 5},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Millisecond*200)
					return models.ErrIdleTimeout
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
		},
		{
			name: "success with server activity",
			expectedErr: nil,
			client: mockConn{
				getFdFunc: func() int {return 3},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Millisecond*200)
					return models.ErrIdleTimeout
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
			server: mockConn{
				getFdFunc: func() int {return 5},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter == 0 {
						requestCounter++
						return nil
					}
					return models.ErrIdleTimeout
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
		},
		{
			name: "fail ErrClientSide",
			expectedErr: models.ErrClientSide,
			client: mockConn{
				getFdFunc: func() int {return 3},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Second*1)
					return errors.New("test err")
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
			server: mockConn{
				getFdFunc: func() int {return 5},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Millisecond*300)
					return nil
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
		},
		{
			name: "fail serverSide",
			expectedErr: models.ErrNoConnsAvailable,
			client: mockConn{
				getFdFunc: func() int {return 3},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Millisecond*300)
					return nil
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
			server: mockConn{
				getFdFunc: func() int {return 5},
				logActivityFunc: func() {},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					time.Sleep(time.Second*1)
					return models.ErrNoConnsAvailable
				},
				lastActivityFunc: func() <-chan time.Time {
					ch := make(chan time.Time)
					go func () {
						ch <- time.Now()
					}()
					return ch
				},
				closeFunc: func() {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testChat := &chat{
				id: "test",
				mu: sync.RWMutex{},
				mainTimeout: time.Millisecond*500,
				idleTimeout: time.Second*300,
				ctlChan: make(chan struct{}),

				client: tc.client,
				server: tc.server,
			}

			err := testChat.tcpProxy()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
			requestCounter = 0
		})
	}
}