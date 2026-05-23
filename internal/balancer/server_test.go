package balancer

import (
	"testing"

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
				MaxSocksPoolLen: 10,
				InitialSocksPoolLen: 5,
			},
			n: mockNetworker{
				newTCPConnFunc: func(a models.Address) (models.Conn, error) {
					return mockConn{}, nil
				},
			},
			expectedErr: nil,
		},
		{
			name: "success no pool",
			opts: &models.ServerOptions{
				DisableSocksPool: true,
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
				MaxSocksPoolLen: 10,
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
				MaxSocksPoolLen: 10,
				InitialSocksPoolLen: 5,
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
					return mockConn{}, nil
				},
			},
		},
		{
			name: "success no pool",
			expectedErr: nil,
			opts: &models.ServerOptions{
				MaxClientsAmount: 10,
				DisableSocksPool: true,
			},
			n: mockNetworker{
				newTCPConnFunc: func(_ models.Address) (models.Conn, error) {
					return mockConn{}, nil
				},
			},
		},
		{
			name: "fail ErrNoConnsAvailable",
			expectedErr: models.ErrNoConnsAvailable,
			opts: &models.ServerOptions{
				DisableSocksPool: true,
			},
			n: mockNetworker{
				newTCPConnFunc: func(_ models.Address) (models.Conn, error) {
					return mockConn{}, nil
				},
			},
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