package mysql

import (
	"github.com/mishudark/eventhus"
	"fmt"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
)

//AggregateDB defines the collection to store the aggregate with their events
type AggregateDB struct {
	ID      [16]byte    `json:"_id"`
	Version int       `json:"version"`
	// Events  []EventDB `json:"events"`
}

//EventDB defines the structure of the events to be stored
type EventDB struct {
	Type          string      `json:"event_type"`
	AggregateID   [16]byte      `json:"_id"`
	//RawData       json.Raw    `json:"data,omitempty"`
	data          interface{} `json:"-"`
	Timestamp     int64   `json:"timestamp"`
	AggregateType string      `json:"aggregate_type"`
	Version       int         `json:"version"`
}

//Client for access to mysql
type Client struct {
	db      string
	session *sql.DB
}

//NewClient generates a new client to access to mysql
func NewClient(dsn string) (eventhus.EventStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	cli := &Client{
		"event_store",
		db,
	}

	return cli, nil
}