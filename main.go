package main

import (
	"log"
	"os"

	"github.com/rseleznev/bazik/config"
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

func (c *controller) run(conf *config.Config) {}