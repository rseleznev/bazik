package main

import (
	"log"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/network"
	"github.com/rseleznev/bazik/polling"
)

func main() {
	poller, err := polling.NewEpoll()
	if err != nil {
		log.Fatal(err)
	}
	conf := config.Parse("./config/config_sample.yaml")
	net := network.NewNet(conf[0].Balancer.MainTimeout, poller)
	b := balancer.NewBalancer(conf[0].Balancer, conf[0].Servers, net)
	b.Run()

	// b := balancer.NewBalancer(
	// 	&models.BalancerOptions{
	// 		Addr: models.Address{
	// 			IP: [4]byte{127, 0, 0, 1},
	// 			Port: 9000,
	// 		},
	// 		Proto: "tcp",
	// 		BalancingAlg: "random",
	// 		MainTimeout: 500,
	// 		RetryAmount: 0,
	// 		MaxClientsAmount: 3,
	// 		MaxIdleSeconds: 300,
	// 		MaxConnsPoolLen: 10,
	// 		InitialConnsPoolLen: 3,
	// 	}, 
	// 	[]*models.ServerOptions{
	// 		{
	// 			Addr: models.Address{
	// 				IP: [4]byte{127, 0, 0, 1},
	// 				Port: 6379,
	// 			},
	// 			MaxClientsAmount: 3,
	// 			MaxIdleSeconds: 300,
	// 			MaxConnsPoolLen: 10,
	// 			InitialConnsPoolLen: 3,
	// 		},
	// 	},
	// 	net)
}