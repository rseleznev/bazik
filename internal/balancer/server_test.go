package balancer

import (
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

var testServer = &server{}

func Test_init(t *testing.T) {
	testCases := []struct{
		name string
		opts *models.ServerOptions
		n mockNetworker // из файла tcp_balancer_test.go
		expectedErr error
	}{
		{
			name: "success pool",
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 10,
				InitialConnsPoolLen: 5,
			},
			n: mockNetworker{
				newTCPConnFunc: func(a models.Address) (models.Conn, error) {
					return mockConn{
						connectFunc: func() error {return nil},
					}, nil
				},
			},
			expectedErr: nil,
		},
		{
			name: "success no pool",
			opts: &models.ServerOptions{
				DisableConnsPool: true,
			},
			n: mockNetworker{
				newTCPConnFunc: func(a models.Address) (models.Conn, error) {
					return mockConn{}, nil
				},
			},
			expectedErr: nil,
		},
		{
			name: "success pool with 0 initial",
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 10,
			},
			n: mockNetworker{
				newTCPConnFunc: func(a models.Address) (models.Conn, error) {
					return mockConn{}, nil
				},
			},
			expectedErr: nil,
		},
		{
			name: "fail ErrNoConnsAvailable",
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 10,
				InitialConnsPoolLen: 5,
			},
			n: mockNetworker{
				newTCPConnFunc: func(a models.Address) (models.Conn, error) {
					return nil, models.ErrNoConnsAvailable
				},
			},
			expectedErr: models.ErrNoConnsAvailable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testServer.opts = tc.opts
			testServer.net = tc.n

			err := testServer.init()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
		})
	}
}

func Test_getConn(t *testing.T) {
	testCases := []struct{
		name string
		expectedErr error
		opts *models.ServerOptions
		n mockNetworker // из файла tcp_balancer_test.go
		setupFunc func()
	}{
		{
			name: "success from pool",
			expectedErr: nil,
			opts: &models.ServerOptions{},
			setupFunc: func() {
				testServer.connPool <- mockConn{}
			},
		},
		{
			name: "success pool zero len",
			expectedErr: nil,
			opts: &models.ServerOptions{
				MaxClientsAmount: 10,
			},
			n: mockNetworker{
				newTCPConnFunc: func(_ models.Address) (models.Conn, error) {
					return mockConn{
						setMainTimeoutFunc: func(_ time.Duration) {},
						connectFunc: func() error {return nil},
					}, nil
				},
			},
		},
		{
			name: "success no pool",
			expectedErr: nil,
			opts: &models.ServerOptions{
				MaxClientsAmount: 10,
				DisableConnsPool: true,
			},
			n: mockNetworker{
				newTCPConnFunc: func(_ models.Address) (models.Conn, error) {
					return mockConn{
						setMainTimeoutFunc: func(_ time.Duration) {},
						connectFunc: func() error {return nil},
					}, nil
				},
			},
		},
		{
			name: "fail ErrNoConnsAvailable",
			expectedErr: models.ErrNoConnsAvailable,
			opts: &models.ServerOptions{
				DisableConnsPool: true,
			},
			n: mockNetworker{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testServer.opts = tc.opts
			testServer.net = tc.n
			if tc.setupFunc != nil {
				tc.setupFunc()
			}

			_, err := testServer.getConn()
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
		})
	}
}

func Test_storeConn(t *testing.T) {
	testCases := []struct{
		name string
		expectedPoolLen int
		opts *models.ServerOptions
		conn mockConn
		setupFunc func()
		cleanupFunc func()
	}{
		{
			name: "full path",
			expectedPoolLen: 1,
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 10,
			},
			conn: mockConn{
				checkUnreadFunc: func() (int, error) {
					return 0, nil
				},
				checkUnsentFunc: func() (int, error) {
					return 0, nil
				},
			},
			setupFunc: func() {
				testServer.connPool = make(chan models.Conn, 10)
			},
			cleanupFunc: func() {
				testServer.connPool = nil
			},
		},
		{
			name: "no store - unread/unsent",
			expectedPoolLen: 0,
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 10,
			},
			conn: mockConn{
				checkUnreadFunc: func() (int, error) {
					return 0, nil
				},
				checkUnsentFunc: func() (int, error) {
					return 15, nil
				},
			},
		},
		{
			name: "no store - max pool len",
			expectedPoolLen: 5,
			opts: &models.ServerOptions{
				MaxConnsPoolLen: 5,
			},
			conn: mockConn{
				checkUnreadFunc: func() (int, error) {
					return 0, nil
				},
				checkUnsentFunc: func() (int, error) {
					return 0, nil
				},
			},
			setupFunc: func() {
				testServer.connPool = make(chan models.Conn, 5)

				for range 5 {
					testServer.connPool <- mockConn{}
				}
			},
			cleanupFunc: func() {
				testServer.connPool = nil
			},
		},
		{
			name: "no store - no pool",
			expectedPoolLen: 0,
			opts: &models.ServerOptions{
				DisableConnsPool: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testServer.opts = tc.opts
			if tc.setupFunc != nil {
				tc.setupFunc()
			}

			testServer.storeConn(tc.conn)
			if n := len(testServer.connPool); n != tc.expectedPoolLen {
				t.Errorf("Ожидаемый размер пула: %d, фактический: %d", tc.expectedPoolLen, n)
			}
			if tc.cleanupFunc != nil {
				tc.cleanupFunc()
			}
		})
	}
}