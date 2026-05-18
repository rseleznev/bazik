package main

import (
	"log"

	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/models"
	"github.com/rseleznev/bazik/internal/network"
	"github.com/rseleznev/bazik/polling"
)

func main() {
	poller, err := polling.NewEpoll()
	if err != nil {
		log.Fatal(err)
	}
	net := network.NewNet(poller)

	// парсим конфиг

	b := balancer.NewBalancer(
		&models.BalancerOptions{
			Addr: models.Address{
				IP: [4]byte{127, 0, 0, 1},
				Port: 9000,
			},
			Proto: "tcp",
			BalancerAlg: "random",
			RetryAmount: 0,
			MaxClientsAmount: 3,
			MaxIdleSeconds: 300,
			MaxSocksPoolLen: 10,
			InitialSocksPoolLen: 3,
		}, 
		[]*models.ServerOptions{
			{
				Addr: models.Address{
					IP: [4]byte{127, 0, 0, 1},
					Port: 6379,
				},
				MainTimeout: 500,
				RetryAmount: 0,
				MaxClientsAmount: 3,
				MaxIdleSeconds: 300,
				MaxSocksPoolLen: 10,
				InitialSocksPoolLen: 3,
			},
		},
		net)
	b.Run()
}