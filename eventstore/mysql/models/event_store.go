package models

import (
	"database/sql"
	"fmt"

	"gitlab.com/vlopes45/dayim"
)

const (
	selectEventStore = "`timestamp`, `aggregate_uuid`, `aggregate_version`, `event_type_id`, `event_data`, `event_meta`"
)

// EventStore represents a row in the event_store table
type EventStore struct {
	Timestamp        int64             `json:"timestamp"`
	AggregateUUID    [16]byte          `json:"aggregate_uuid"`
	AggregateVersion int               `json:"aggregate_version"`
	EventTypeID      dayim.EventTypeID `json:"event_type_id"`
	EventData        dayim.EventData   `json:"event_data"`
	EventMeta        dayim.EventMeta   `json:"event_meta"`
	offset           int
	limit            int
}

// Insert a new EventStore row in the event_store table
func (e *EventStore) Insert(qu Queryer) (lastInsertID int64, err error) {
	const stmt = "INSERT INTO `event_store` (`timestamp`, `aggregate_uuid`, `aggregate_version`, `event_type_id`, `event_data`, `event_meta`) VALUES (?, ?, ?, ?, ?, ?)"
	res, err := qu.Exec(stmt, e.Timestamp, e.AggregateUUID, e.AggregateVersion, e.EventTypeID, e.EventData, e.EventMeta)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (e *EventStore) PrepareInsertStmt(tx *sql.Tx) (stmt *sql.Stmt, err error) {
	stmt, err = tx.Prepare("INSERT INTO `event_store` (`timestamp`, `aggregate_uuid`, `aggregate_version`, `event_type_id`, `event_data`, `event_meta`) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

// Find an existing EventStore row in the event_store table
func (e *EventStore) Find(qu Queryer, id int64) error {
	const stmt = "SELECT " + selectEventStore + " FROM `event_store` WHERE id = ?"
	row := qu.QueryRow(stmt, id)
	return row.Scan(&e.Timestamp, &e.AggregateUUID, &e.AggregateVersion, &e.EventTypeID, &e.EventData, &e.EventMeta)
}

// Load all, or a subset of EventStore rows from the event_store table
func (e *EventStore) Load(qu Queryer) (set []EventStore, err error) {
	stmt := "SELECT " + selectEventStore + " FROM `event_store`"

	if e.limit == 0 && e.offset > 0 {
		return set, fmt.Errorf("cannot query with offset but no limit")
	}

	if e.limit > 0 {
		stmt += fmt.Sprintf(" LIMIT %d", e.limit)
	}
	if e.offset > 0 {
		stmt += fmt.Sprintf(" OFFSET %d", e.offset)
	}
	defer func() {
		e.limit = 0
		e.offset = 0
	}()
	rows, err := qu.Query(stmt)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var e EventStore
		if err = rows.Scan(&e.Timestamp, &e.AggregateUUID, &e.AggregateVersion, &e.EventTypeID, &e.EventData, &e.EventMeta); err != nil {
			return
		}
		set = append(set, e)
	}

	return
}

func (e *EventStore) LoadByAggregate(qu Queryer, uuid []byte) (set []EventStore, err error) {
	stmt := "SELECT " + selectEventStore + " FROM `event_store` WHERE uuid = ?"

	if e.limit == 0 && e.offset > 0 {
		return set, fmt.Errorf("cannot query with offset but no limit")
	}

	if e.limit > 0 {
		stmt += fmt.Sprintf(" LIMIT %d", e.limit)
	}
	if e.offset > 0 {
		stmt += fmt.Sprintf(" OFFSET %d", e.offset)
	}
	defer func() {
		e.limit = 0
		e.offset = 0
	}()
	rows, err := qu.Query(stmt, uuid)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var e EventStore
		if err = rows.Scan(&e.Timestamp, &e.AggregateUUID, &e.AggregateVersion, &e.EventTypeID, &e.EventData, &e.EventMeta); err != nil {
			return
		}
		set = append(set, e)
	}

	return
}

// Count the number of rows from the event_store table
func (e *EventStore) Count(qu Queryer) (count int64, err error) {
	const stmt = "SELECT COUNT(*) FROM `event_store`"
	row := qu.QueryRow(stmt)
	if err = row.Scan(&count); err != nil {
		return
	}
	return
}

// Exists checks for the items existence in the database, based on it's id.
// An error will only be returned if a SQL related failure happens.
// In all other cases, a bool and nil will return.
func (e *EventStore) Exists(qu Queryer, id int64) (exists bool, err error) {
	const stmt = "SELECT EXISTS(SELECT 1 FROM `event_store` WHERE id = ? LIMIT 1) AS `exists`"
	var count int
	row := qu.QueryRow(stmt, id)
	if err = row.Scan(&count); err != nil {
		return
	}
	return count > 0, nil
}

// TableName returns the table name
func (e *EventStore) TableName() string {
	return "event_store"
}

// SetLimit sets the query limit
func (e *EventStore) SetLimit(limit int) *EventStore {
	e.limit = limit
	return e
}

// SetOffset sets the query offset
func (e *EventStore) SetOffset(offset int) *EventStore {
	e.offset = offset
	return e
}
