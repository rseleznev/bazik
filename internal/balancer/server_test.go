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
			name: "success",
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