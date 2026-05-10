package main

import (
	"log"
	"os"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/handler"
	"github.com/rseleznev/bazik/internal/models"
)

func main() {
	// создаем controller
	ctl := controller{}
	
	// парсим stdin (флаги, ссылку на конфиг)
	
	// парсим конфиг
	conf := ctl.parseConfig(ctl.flags[0])

	// запускаем
	ctl.run(conf)
}

type controller struct {
	flags []string

	blncr balancer.Balancer
}

func (c *controller) parseConfig(path string) *config.Config {
	_, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	
	conf := &config.Config{
		Proto: "random",
	}
	
	return conf
}

func (c *controller) run(conf *config.Config) {
	// создаем Handler
	var p mockPoller
	h := handler.NewHandler(p)
	
	// создаем Balancer
	c.blncr = balancer.NewBalancer(conf, h)

	// слушаем входящие соединения
	c.blncr.Start()
}

type mockPoller struct {}
func (mp mockPoller) Add(_ models.PollingUnit) error