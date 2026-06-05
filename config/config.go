package config

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/rseleznev/bazik/internal/models"
)

type Config struct {	
	IP string `yaml:"ip"`
	Port int `yaml:"port"`

	// Протокол/уровень балансировки (tcp/udp/http)
	Proto string `yaml:"proto"`

	// Алгоритм балансировки
	BalancingAlg string `yaml:"balancing_alg"`

	// Режим проксирования
	//
	// 	- zero-copy - прямой перенос данных между сокетами без копирования в user space (возможна потеря данных)
	// 	- guaranteed delivery - данные копируются в user space, новые сообщения от отправителя 
	// 	не принимаются (поток в обратную сторону продолжает работу), пока не будет получен ACK
	ProxyMode string `yaml:"proxy_mode"`

	// Время в миллисекундах, за которое должна выполняться каждая операция.
	// Если по истечении таймаута операция не будет выполнена, это будет расценено как ошибка.
	// Если разрешены ретраи (RetryAmount), будет попытка найти другой сервер. Иначе - соединение будет закрыто
	//
	// По умолчанию 100 мс
	MainTimeout int `yaml:"main_timeout"`
	// Время в миллисекундах, за которое должен прийти ACK на отправленный пакет (TCP_USER_TIMEOUT)
	//
	// 0 - системный дефолт (15-20 минут)
	TCP_ACK_Timeout int
	// Количество попыток переподключиться к серверу при ошибке.
	//
	// Если необходима целостность данных, следует установить 0,
	// в таком случае при первой же ошибке соединение с клиентом будет закрыто
	// и клиент должен будет начать передачу заново
	//
	// Если RetryAmount >= 1, может нарушиться целостность, т.к.
	// передача другому серверу продолжится со следующего полученного сообщения клиента
	// с потерей полученных ранее и не доставленных данных
	RetryAmount int `yaml:"retry_amount"`

	// Частота проверки жизни серверов
	// Размеры буферов ядра

	// ------------------------------------
	// Общие настройки для всех серверов
	// У серверов аналогичные настройки, которые имеют приоритет над общими

	// Максимальное кол-во клиентов.
	//
	// Когда лимит будет превышен, последующие клиенты будут получать ошибку ECONNREFUSED,
	// пока кол-во активных соединений не будет уменьшено
	//
	// Эпизодически может немного превышаться
	MaxClientsAmount int `yaml:"max_clients_amount"`

	// Максимальное время бездействия соединения
	MaxIdleSeconds int `yaml:"max_idle_seconds"`

	// Отключение пула серверных соединений у ВСЕХ серверов. Если у некоторых серверов должен быть пул,
	// а у других нет - данное поле заполнять не нужно, вместо этого заполнить MaxConnsPoolLen и InitialConnsPoolLen
	// на уровне серверов
	//
	// По умолчанию false, то есть пул создается у тех серверов, где MaxConnsPoolLen и InitialConnsPoolLen > 0
	DisablePoolMainFlag bool `yaml:"disable_pool_main_flag"`
	// Максимальные размеры пула соединений для каждого сервера.
	// Должен быть больше 0.
	// 
	// По умолчанию 10
	MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
	// Начальное количество соединений в пуле для каждого сервера.
	// Количество может увеличиваться до MaxServerConnsPoolLen в зависимости
	// от нагрузки, и потом снова снижается до InitialServerConnsPoolLen.
	// Не должен быть больше MaxServerConnsPoolLen.
	// 
	// По умолчанию 3
	InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`

	// ------------------------------------

	// Список доступных серверов
	Servers []struct {
		Address string `yaml:"address"`

		MaxClientsAmount int `yaml:"max_clients_amount"`
		MaxIdleSeconds int `yaml:"max_idle_seconds"`
		DisableConnsPool bool `yaml:"disable_conns_pool"`
		MaxConnsPoolLen int `yaml:"max_conns_pool_len"`
		InitialConnsPoolLen int `yaml:"initial_conns_pool_len"`
	}
}

type BalancerConfig struct {
	Balancer *models.BalancerOptions
	Servers []*models.ServerOptions
}

func Parse(path string) []BalancerConfig {
	balancerConf := make([]BalancerConfig, 0, 3)
	srvOptions := make([]*models.ServerOptions, 0, 10)
	d, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	c := &Config{}
	yaml.Unmarshal(d, c)
	err = validateConfig(c)
	if err != nil {
		log.Fatal(err)
	}

	portStr := strconv.Itoa(c.Port)
	ipBytes, err := parseIp(c.IP)
	if err != nil {
		log.Fatal(err)
	}
	bO := &models.BalancerOptions{
		Addr: models.Address{
			Raw: c.IP + ":" + portStr,
			IP: ipBytes,
			Port: c.Port,
		},
		Proto: c.Proto,
		BalancingAlg: c.BalancingAlg,
		MainTimeout: c.MainTimeout,
		RetryAmount: c.RetryAmount,
		MaxClientsAmount: c.MaxClientsAmount,
		MaxIdleSeconds: c.MaxIdleSeconds,
		MaxConnsPoolLen: c.MaxConnsPoolLen,
		InitialConnsPoolLen: c.InitialConnsPoolLen,
	}
	for _, v := range c.Servers {
		ipBytes, port, err := parseIpAndPort(v.Address)
		if err != nil {
			log.Fatal(err)
		}
		sO := &models.ServerOptions{
			Addr: models.Address{
				Raw: v.Address,
				IP: ipBytes,
				Port: port,
			},
			MainTimeout: c.MainTimeout,
			MaxClientsAmount: v.MaxClientsAmount,
			MaxIdleSeconds: v.MaxIdleSeconds,
			DisableConnsPool: v.DisableConnsPool,
			MaxConnsPoolLen: v.MaxConnsPoolLen,
			InitialConnsPoolLen: v.InitialConnsPoolLen,
		}
		srvOptions = append(srvOptions, sO)
	}
	balancerConf = append(balancerConf, BalancerConfig{
		Balancer: bO,
		Servers: srvOptions,
	})
	
	return balancerConf
}

func parseIp(raw string) ([4]byte, error) {
	var result [4]byte
	buf := make([]byte, 0, 3)
	var i, r int

	for i < len(raw) {
		if raw[i] == '.' {
			i++
			n, err := strconv.Atoi(string(buf))
			if err != nil {
				return [4]byte{}, err
			}
			result[r] = byte(n)
			buf = buf[:0]
			r++
			continue
		}
		buf = append(buf, raw[i])
		i++
	}
	n, err := strconv.Atoi(string(buf))
	if err != nil {
		return [4]byte{}, err
	}
	result[r] = byte(n)
	return result, nil
}

func parseIpAndPort(raw string) ([4]byte, int, error) {
	var result [4]byte
	buf := make([]byte, 0, 3)
	var i, r, p int

	for i < len(raw) {
		if raw[i] == '.' {
			i++
			n, err := strconv.Atoi(string(buf))
			if err != nil {
				return [4]byte{}, 0, err
			}
			result[r] = byte(n)
			buf = buf[:0]
			r++
			continue
		}
		if raw[i] == ':' {
			i++
			n, err := strconv.Atoi(string(buf))
			if err != nil {
				return [4]byte{}, 0, err
			}
			result[r] = byte(n)

			p, err = strconv.Atoi(string(raw[i:]))
			if err != nil {
				return [4]byte{}, 0, err
			}
			break
		}
		buf = append(buf, raw[i])
		i++
	}
	return result, p, nil
}

func validateConfig(c *Config) error {
	if len(c.IP) > 15 {
		return models.ErrTooLongIpAddress
	}
	if len(c.IP) < 7 {
		return models.ErrTooShortIpAddress
	}
	if c.Port >= 66_000 {
		return models.ErrPortNumOutOfRange
	}
	if c.Port <= 0 {
		return models.ErrPortInvalid
	}
	switch c.Proto {
	case "tcp":

	default:
		return models.ErrWrongProto

	}

	switch c.BalancingAlg {
	case "random", "round robin":

	default:
		return models.ErrUnsupportedBalancingAlg
	}

	switch c.ProxyMode {
	case "zero-copy", "guaranteed delivery":

	default:
		return models.ErrUnsupportedProxyMode
	}

	if c.MainTimeout <= 0 {
		c.MainTimeout = 100
	}
	if c.MaxClientsAmount < 0 {
		return fmt.Errorf("err max_client_amount: %w", models.ErrNumInvalid)
	}
	if c.MaxIdleSeconds < 0 {
		return fmt.Errorf("err max_idle_seconds: %w", models.ErrNumInvalid)
	}
	if c.MaxConnsPoolLen < 0 {
		return fmt.Errorf("err max_conns_pool_len: %w", models.ErrNumInvalid)
	}
	if c.InitialConnsPoolLen < 0 {
		return fmt.Errorf("err initial_conns_pool_len: %w", models.ErrNumInvalid)
	}

	for i, v := range c.Servers {
		if len(v.Address) > 21 {
			return models.ErrTooLongIpAddress
		}
		if len(v.Address) < 9 {
			return models.ErrTooShortIpAddress
		}
		if v.MaxClientsAmount <= 0 {
			if c.MaxClientsAmount == 0 {
				return models.ErrNoMaxClientsAmount
			}
			c.Servers[i].MaxClientsAmount = c.MaxClientsAmount
		}
		if v.MaxIdleSeconds <= 0 {
			if c.MaxIdleSeconds == 0 {
				return models.ErrNoMaxIdleSeconds
			}
			c.Servers[i].MaxIdleSeconds = c.MaxIdleSeconds
		}
		if !c.DisablePoolMainFlag && !v.DisableConnsPool {
			if v.MaxConnsPoolLen <= 0 {
				if c.MaxConnsPoolLen == 0 {
					c.MaxConnsPoolLen = 10
				}
				c.Servers[i].MaxConnsPoolLen = c.MaxConnsPoolLen
			}
			if v.InitialConnsPoolLen <= 0 {
				if c.InitialConnsPoolLen == 0 {
					c.InitialConnsPoolLen = 3
				}
				c.Servers[i].InitialConnsPoolLen = c.InitialConnsPoolLen
			}
		}
	}
	
	return nil
}