package balancer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

func Test_tcpProxy(t *testing.T) {
	var requestCounter atomic.Int32
	var cancelChan chan struct{}
	
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
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 3},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {return models.ErrIdleTimeout},
				closeFunc: func() {},
			},
			server: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 5},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {return models.ErrIdleTimeout},
				closeFunc: func() {},
			},
		},
		{
			name: "success with client activity",
			expectedErr: nil,
			client: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 3},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						time.Sleep(time.Millisecond*50)
						requestCounter.Add(1)
						return nil
					}
					return models.ErrIdleTimeout
				},
				closeFunc: func() {},
			},
			server: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 5},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						return nil
					}
					return models.ErrPollCancel
				},
				closeFunc: func() {},
			},
		},
		{
			name: "success with server activity",
			expectedErr: nil,
			client: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 3},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						return nil
					}
					return models.ErrPollCancel
				},
				closeFunc: func() {},
			},
			server: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 5},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						time.Sleep(time.Millisecond*50)
						requestCounter.Add(1)
						return nil
					}
					return models.ErrIdleTimeout
				},
				closeFunc: func() {},
			},
		},
		{
			name: "fail ErrClientSide",
			expectedErr: models.ErrClientSide,
			client: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 3},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						requestCounter.Add(1)
						return nil
					}
					return errors.New("test err")
				},
				closeFunc: func() {},
			},
			server: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(ch chan struct{}) {
					cancelChan = ch
				},
				getFdFunc: func() int {return 5},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					<-cancelChan
					return models.ErrPollCancel
				},
				closeFunc: func() {},
			},
		},
		{
			name: "fail serverSide",
			expectedErr: models.ErrNoConnsAvailable,
			client: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(ch chan struct{}) {
					cancelChan = ch
				},
				getFdFunc: func() int {return 3},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					<-cancelChan
					return models.ErrPollCancel
				},
				closeFunc: func() {},
			},
			server: mockConn{
				withTimerFunc: func(_ *time.Timer) {},
				withCancelFunc: func(_ chan struct{}) {},
				getFdFunc: func() int {return 5},
				setIdleTimeoutFunc: func(_ time.Duration) {},
				setMainTimeoutFunc: func(_ time.Duration) {},
				copyToFunc: func(_ models.Conn) error {
					if requestCounter.Load() == 0 {
						requestCounter.Add(1)
						return nil
					}
					return models.ErrNoConnsAvailable
				},
				closeFunc: func() {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testChat := &chat{
				id: "test",
				mu: sync.Mutex{},
				mainTimeout: time.Millisecond*500,
				idleTimeout: time.Second*300,

				client: tc.client,
				server: tc.server,
			}

			err := testChat.tcpProxy()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
			if n := requestCounter.Load(); n > 0 {
				requestCounter.Add(-n)
			}
		})
	}
}