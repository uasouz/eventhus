package config

import (
	"github.com/uasouz/eventhus"
	"github.com/uasouz/eventhus/commandbus/async"
	"github.com/uasouz/eventhus/eventbus/mosquitto"
	"github.com/uasouz/eventhus/eventbus/nats"
	"github.com/uasouz/eventhus/eventbus/rabbitmq"
	"github.com/uasouz/eventhus/eventstore/badger"
	"github.com/uasouz/eventhus/eventstore/mongo"
	"github.com/uasouz/eventhus/eventstore/mysql"
)

// EventBus returns an eventhus.EventBus impl
type EventBus func() (eventhus.EventBus, error)

// EventStore returns an eventhus.EventStore impl
type EventStore func() (eventhus.EventStore, error)

// CommandBus returns an eventhus.CommandBus
type CommandBus func(register eventhus.CommandHandlerRegister) (eventhus.CommandBus, error)

// CommandConfig should connect internally commands with an aggregate
type CommandConfig func(repository *eventhus.Repository, register *eventhus.CommandRegister)

// commandHandler is the signature used by command handlers constructor
type commandHandler func(repository *eventhus.Repository, aggregate eventhus.AggregateHandler, bucket, subset string) eventhus.CommandHandle

// WireCommands acts as a wired between aggregate, register and commands
func WireCommands(aggregate eventhus.AggregateHandler, handler commandHandler, bucket, subset string, commands ...interface{}) CommandConfig {
	return func(repository *eventhus.Repository, register *eventhus.CommandRegister) {
		h := handler(repository, aggregate, bucket, subset)
		for _, command := range commands {
			register.Add(command, h)
		}
	}
}

// NewClient returns a command bus properly configured
func NewClient(es EventStore, eb EventBus, cb CommandBus, cmdConfigs ...CommandConfig) (eventhus.CommandBus, error) {
	store, err := es()
	if err != nil {
		return nil, err
	}

	bus, err := eb()
	if err != nil {
		return nil, err
	}

	repository := eventhus.NewRepository(store, bus)
	register := eventhus.NewCommandRegister()

	for _, conf := range cmdConfigs {
		conf(repository, register)
	}
	return cb(register)
}

// RabbitMq generates a RabbitMq implementation of EventBus
func RabbitMq(username, password, host string, port int) EventBus {
	return func() (eventhus.EventBus, error) {
		return rabbitmq.NewClient(username, password, host, port)
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(urls string, useTLS bool) EventBus {
	return func() (eventhus.EventBus, error) {
		return nats.NewClient(urls, useTLS)
	}
}

// Mosquitto generates a Mosquitto implementation of EventBus
func Mosquitto(method string, host string, port int, clientID string) EventBus {
	return func() (eventhus.EventBus, error) {
		return mosquitto.NewClientWithPort(method, host, port, clientID)
	}
}

// Mongo generates a MongoDB implementation of EventStore
func Mongo(host string, port int, db string) EventStore {
	return func() (eventhus.EventStore, error) {
		return mongo.NewClient(host, port, db)
	}
}

func Mysql(dsn string) EventStore {
	return func() (eventhus.EventStore, error) {
		return mysql.NewClient(dsn)
	}
}

// Badger generates a BadgerDB implementation of EventStore
func Badger(dbDir string) EventStore {
	return func() (eventhus.EventStore, error) {
		return badger.NewClient(dbDir)
	}
}

// AsyncCommandBus generates a CommandBus
func AsyncCommandBus(workers int) CommandBus {
	return func(register eventhus.CommandHandlerRegister) (eventhus.CommandBus, error) {
		return async.NewBus(register, workers), nil
	}
}
