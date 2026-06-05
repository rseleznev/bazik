package config

import (
	"testing"

	"github.com/rseleznev/bazik/internal/models"
)

func Test_validateConfig(t *testing.T) {
	testCases := []struct{
		name string
		expectedErr error
		cfg *Config
	}{
		{
			name: "err ErrTooLongIpAddress",
			expectedErr: models.ErrTooLongIpAddress,
			cfg: &Config{
				IP: "255.255.255.2555",
			},
		},
		{
			name: "err ErrTooShortIpAddress",
			expectedErr: models.ErrTooShortIpAddress,
			cfg: &Config{
				IP: "0.0.0.",
			},
		},
		{
			name: "err ErrPortNumOutOfRange",
			expectedErr: models.ErrPortNumOutOfRange,
			cfg: &Config{
				IP: "0.0.0.0",
				Port: 67000,
			},
		},
		{
			name: "err ErrPortInvalid",
			expectedErr: models.ErrPortInvalid,
			cfg: &Config{
				IP: "0.0.0.0",
				Port: 0,
			},
		},
		{
			name: "err ErrWrongProto",
			expectedErr: models.ErrWrongProto,
			cfg: &Config{
				IP: "255.255.255.255",
				Port: 123,
				Proto: "wrong",
			},
		},
		{
			name: "err ErrUnsupportedBalancingAlg",
			expectedErr: models.ErrUnsupportedBalancingAlg,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "least connections",
			},
		},

		{
			name: "err ErrUnsupportedProxyMode",
			expectedErr: models.ErrUnsupportedProxyMode,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "wrong",
			},
		},
		{
			name: "err server ErrTooLongIpAddress",
			expectedErr: models.ErrTooLongIpAddress,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "255.255.255.255:100000"},
				},
			},
		},
		{
			name: "err server ErrTooShortIpAddress",
			expectedErr: models.ErrTooShortIpAddress,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "0.0.0.:1"},
				},
			},
		},
		{
			name: "err ErrNoMaxClientsAmount",
			expectedErr: models.ErrNoMaxClientsAmount,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "127.0.0.1:6379"},
				},
			},
		},
		{
			name: "err ErrNoMaxIdleSeconds",
			expectedErr: models.ErrNoMaxIdleSeconds,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				MaxClientsAmount: 10,
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "127.0.0.1:6379"},
				},
			},
		},
		{
			name: "success",
			expectedErr: nil,
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				MaxClientsAmount: 10,
				MaxIdleSeconds: 300,
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "127.0.0.1:6379"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg)
			if err != tc.expectedErr {
				t.Errorf("Ожидаемая ошибка: %s, получено: %s", tc.expectedErr, err)
			}
		})
	}
}

func Test_validateConfigInit(t *testing.T) {
	testCases := []struct{
		name string
		cfg *Config
		checkFunc func(*Config) bool
	}{
		{
			name: "filled from balancer opts",
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				MaxClientsAmount: 10,
				MaxIdleSeconds: 300,
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{Address: "127.0.0.1:6379"},
				},
			},
			checkFunc: func(c *Config) bool {
				var ok bool = true
				if c.Servers[0].MaxClientsAmount != 10 {
					ok = false
				}
				if c.Servers[0].MaxIdleSeconds != 300 {
					ok = false
				}
				return ok
			},
		},
		{
			name: "zeros balancer opts, servers different",
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{
						Address: "127.0.0.1:6379",
						MaxClientsAmount: 5,
						MaxIdleSeconds: 300,
						MaxConnsPoolLen: 20,
						InitialConnsPoolLen: 5,
					},
					{
						Address: "127.0.0.1:7000",
						MaxClientsAmount: 20,
						MaxIdleSeconds: 500,
						MaxConnsPoolLen: 50,
						InitialConnsPoolLen: 15,
					},
				},
			},
			checkFunc: func(c *Config) bool {
				var ok bool = true
				for i, v := range c.Servers {
					if i == 0 {
						if v.MaxClientsAmount != 5 {ok = false}
						if v.MaxIdleSeconds != 300 {ok = false}
						if v.MaxConnsPoolLen != 20 {ok = false}
						if v.InitialConnsPoolLen != 5 {ok = false}
					}
					if i == 1 {
						if v.MaxClientsAmount != 20 {ok = false}
						if v.MaxIdleSeconds != 500 {ok = false}
						if v.MaxConnsPoolLen != 50 {ok = false}
						if v.InitialConnsPoolLen != 15 {ok = false}
					}
				}
				return ok
			},
		},
		{
			name: "filled from balancer opts, one server no pool",
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				MaxClientsAmount: 5,
				MaxIdleSeconds: 300,
				InitialConnsPoolLen: 3,
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{
						Address: "127.0.0.1:6379",
						MaxConnsPoolLen: 20,
					},
					{
						Address: "127.0.0.1:7000",
						MaxClientsAmount: 20,
						MaxIdleSeconds: 500,
						DisableConnsPool: true,
					},
				},
			},
			checkFunc: func(c *Config) bool {
				var ok bool = true
				for i, v := range c.Servers {
					if i == 0 {
						if v.MaxClientsAmount != 5 {ok = false}
						if v.MaxIdleSeconds != 300 {ok = false}
						if v.MaxConnsPoolLen != 20 {ok = false}
						if v.InitialConnsPoolLen != 3 {ok = false}
					}
					if i == 1 {
						if v.MaxClientsAmount != 20 {ok = false}
						if v.MaxIdleSeconds != 500 {ok = false}
						if v.MaxConnsPoolLen != 0 {ok = false}
						if v.InitialConnsPoolLen != 0 {ok = false}
					}
				}
				return ok
			},
		},
		{
			name: "filled from balancer opts, no pool",
			cfg: &Config{
				IP: "192.168.0.50",
				Port: 8000,
				Proto: "tcp",
				BalancingAlg: "random",
				ProxyMode: "zero-copy",
				MaxClientsAmount: 5,
				MaxIdleSeconds: 300,
				DisablePoolMainFlag: true,
				Servers: []struct{
					Address string `yaml:"address"`
					MaxClientsAmount int `yaml:"max_clients_amount"`
					MaxIdleSeconds int `yaml:"max_idle_seconds"`
					DisableConnsPool bool `yaml:"disable_conns_pool"`
					MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
					InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
				}{
					{
						Address: "127.0.0.1:6379",
						MaxClientsAmount: 20,
					},
					{
						Address: "127.0.0.1:7000",
						MaxIdleSeconds: 500,
					},
				},
			},
			checkFunc: func(c *Config) bool {
				var ok bool = true
				for i, v := range c.Servers {
					if i == 0 {
						if v.MaxClientsAmount != 20 {ok = false}
						if v.MaxIdleSeconds != 300 {ok = false}
						if v.MaxConnsPoolLen != 0 {ok = false}
						if v.InitialConnsPoolLen != 0 {ok = false}
					}
					if i == 1 {
						if v.MaxClientsAmount != 5 {ok = false}
						if v.MaxIdleSeconds != 500 {ok = false}
						if v.MaxConnsPoolLen != 0 {ok = false}
						if v.InitialConnsPoolLen != 0 {ok = false}
					}
				}
				return ok
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validateConfig(tc.cfg)
			if !tc.checkFunc(tc.cfg) {
				t.Error("Результаты не совпадают")
			}
		})
	}
}