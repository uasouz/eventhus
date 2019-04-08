package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/uasouz/eventhus"
	"time"
)

//AggregateDB defines the collection to store the aggregate with their events
type AggregateDB struct {
	ID      [16]byte `json:"_id"`
	Version int      `json:"version"`
	// Events  []EventDB `json:"events"`
}

//EventDB defines the structure of the events to be stored
type EventDB struct {
	Type        string   `json:"event_type"`
	AggregateID [16]byte `json:"_id"`
	//RawData       json.Raw    `json:"data,omitempty"`
	Data          interface{} `json:"data,omitempty"`
	Timestamp     int64       `json:"timestamp"`
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

func prepareInsertStmt(tx *sql.Tx) (stmt *sql.Stmt, err error) {
	stmt, err = tx.Prepare("INSERT INTO `event_store` (`timestamp`, `aggregate_id`, `version`, `event_type`, `event_data`, `aggregate_type`) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func makeTimestamp() int64 {
	ts := time.Now().UnixNano() / (int64(time.Microsecond) / int64(time.Nanosecond))
	// fmt.Println(ts)
	return ts
}

func eventDBFromEvent(event eventhus.Event) (EventDB, error) {
	aggregateID, err := uuid.Parse(event.AggregateID)
	if err != nil {
		return EventDB{}, err
	}
	return EventDB{
		Type:          event.Type,
		AggregateID:   aggregateID,
		Data:          event.Data,
		Timestamp:     makeTimestamp(),
		AggregateType: event.AggregateType,
		Version:       event.Version,
	}, nil
}

func (c *Client) save(events []eventhus.Event, version int, safe bool) error {
	if len(events) == 0 {
		return errors.New("no Events to Save")
	}
	tx, err := c.session.Begin()

	if err != nil {
		return err
	}

	for _, event := range events {
		var eventStore, err = eventDBFromEvent(event)
		if err != nil {
			return err
		}
		stmt, err := prepareInsertStmt(tx)
		if err != nil {
			return err
		}
		//`timestamp`, `uuid`, `version`, `event_type_id`, `event_data`, `event_meta`
		EvUUID, err := uuid.Parse(event.AggregateID)
		if err != nil {
			return err
		}
		binEvUUID, err := EvUUID.MarshalBinary()
		if err != nil {
			return err
		}

		eventdata,err := json.Marshal(event.Data)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(eventStore.Timestamp, binEvUUID, eventStore.Version, eventStore.Type, eventdata, eventStore.AggregateType)
		if err != nil {
			return err
		}
	}
	tx.Commit()
	return nil

}

//SafeSave store the events without check the current version
func (c *Client) SafeSave(events []eventhus.Event, version int) error {
	return c.save(events, version, true)
}

//Save the events ensuring the current version
func (c *Client) Save(events []eventhus.Event, version int) error {
	return c.save(events, version, false)
}

//Load the stored events for an AggregateID
func (c *Client) Load(aggregateID string) ([]eventhus.Event, error) {
	var events []eventhus.Event
	aggregateUUID, err := uuid.Parse(aggregateID)
	if err != nil {
		return nil, err
	}
	binAggregateID, err := aggregateUUID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	eventsFromDB, err := LoadByAggregate(c.session, binAggregateID)
	events = make([]eventhus.Event, len(eventsFromDB))

	for i, event := range eventsFromDB {
		events[i] = eventhus.Event{
			ID:            aggregateID,
			AggregateID:   aggregateID,
			AggregateType: event.AggregateType,
			Version:       event.Version,
			Type:          event.Type,
			Data:          event.Data,
		}
	}
	return events,nil
}

func LoadByAggregate(qu Queryer, uuid []byte) (set []EventDB, err error) {
	stmt := "SELECT * FROM `event_store` WHERE aggregate_id = ?"

	//if limit == 0 && e.offset > 0 {
	//	return set, errors.New("cannot query with offset but no limit")
	//}
	//
	//if limit > 0 {
	//	stmt += fmt.Sprintf(" LIMIT %d", e.limit)
	//}
	//if offset > 0 {
	//	stmt += fmt.Sprintf(" OFFSET %d", e.offset)
	//}
	//
	//defer func() {
	//	limit = 0
	//	offset = 0
	//}()
	rows, err := qu.Query(stmt, uuid)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var e EventDB
		if err = rows.Scan(&e.Timestamp, &e.AggregateID, &e.Version, &e.Type, &e.Data, &e.AggregateType); err != nil {
			return
		}
		set = append(set, e)
	}

	return
}
