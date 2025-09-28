package evoke

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

type fileStore struct {
	EventRegistry
	mu         sync.Mutex
	db         *sqlx.DB
	publishers []RecordedEventPublisher
}

func NewFileStore(dbFile string) (*fileStore, error) {
	err := os.MkdirAll(filepath.Dir(dbFile), 0755)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}

	// Tune connection pool
	db.SetMaxOpenConns(1) // SQLite supports one writer, so cap to 1
	db.SetMaxIdleConns(1)

	// Enable WAL mode for better concurrency and durability
	if _, err := db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Optional performance pragmas (tweak based on needs):
	if _, err := db.Exec(`PRAGMA synchronous = NORMAL;`); err != nil {
		return nil, fmt.Errorf("failed to set synchronous: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		return nil, fmt.Errorf("failed to enable foreign_keys: %w", err)
	}

	if _, err := db.Exec(`
		create table if not exists events (
			sequence     integer primary key autoincrement,
                        aggregate_id text not null,
                        event_type   text not null,
                        event_json   text not null
		);
	`); err != nil {
		return nil, fmt.Errorf("failed to create events table: %w", err)
	}

	sqlxDB := sqlx.NewDb(db, "sqlite3")

	return &fileStore{
		db:         sqlxDB,
		publishers: []RecordedEventPublisher{},
	}, nil
}

func (s *fileStore) RegisterPublisher(publisher RecordedEventPublisher) {
	s.publishers = append(s.publishers, publisher)
}

// Return all events, not really a long term method here
func (s *fileStore) DebugEvents() ([]RecordedEvent, error) {
	var events []RecordedEvent
	s.db.Select(&events, `select * from events order by sequence asc`)
	return events, nil
}

type dbEvent struct {
	Sequence int64 `db:"sequence"`
	//Timestamp   time.Time `db:"timestamp"`
	AggregateID uuid.UUID `db:"aggregate_id"`
	EventJSON   string    `db:"event_json"`
	EventType   string    `db:"event_type"`
}

func (e *dbEvent) UnmarshalFromRegistry(s EventRegisterer) (RecordedEvent, error) {
	event, err := s.UnmarshalEvent(e.EventType, []byte(e.EventJSON))
	if err != nil {
		return RecordedEvent{}, fmt.Errorf("UnmarshalEvent: %w", err)
	}

	return RecordedEvent{
		Sequence: e.Sequence,
		//Timestamp   time.Time `db:"timestamp"`
		AggregateID: e.AggregateID,
		EventType:   e.EventType,
		Event:       event,
	}, nil
}

func (s *fileStore) appendEvents(aggregateID uuid.UUID, evs []Event) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(evs) == 0 {
		return nil, errors.New("no events to append")
	}

	out := make([]RecordedEvent, 0, len(evs))
	for _, e := range evs {
		eventBytes, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("Marshal: %w", err)
		}

		var row dbEvent
		err = s.db.Get(&row, `insert into events(aggregate_id, event_json, event_type) values(?,?,?) returning *`,
			aggregateID, string(eventBytes), TypeName(e))
		if err != nil {
			return nil, fmt.Errorf("insert into events: %w", err)
		}

		rec, err := row.UnmarshalFromRegistry(s)
		if err != nil {
			return nil, fmt.Errorf("getRecordedEvent: %w", err)
		}

		fmt.Println("filestore rec", rec)

		out = append(out, rec)
	}
	return out, nil
}

func (s *fileStore) Record(aggregateID uuid.UUID, evs []Event) error {
	recs, err := s.appendEvents(aggregateID, evs)
	if err != nil {
		return err
	}

	for _, p := range s.publishers {
		for _, rec := range recs {
			err := p.Publish(rec, false)
			if err != nil {
				return fmt.Errorf("publish: %w", err)
			}
		}
	}

	return nil
}

func (s *fileStore) LoadStream(aggregateID uuid.UUID) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []RecordedEvent
	err := s.db.Select(&events, `select * from events where aggregate_id = ? order by sequence asc`, aggregateID.String())
	if err != nil {
		return nil, fmt.Errorf("select from events: %w", err)
	}
	return events, nil
}

func (s *fileStore) ReplayFrom(seq int64, publisher RecordedEventPublisher) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var rows []dbEvent
	err := s.db.Select(&rows, `select * from events where sequence >= ? order by sequence asc`, seq)
	if err != nil {
		return fmt.Errorf("select from events: %w", err)
	}

	for _, row := range rows {
		rec, err := row.UnmarshalFromRegistry(s)
		if err != nil {
			return fmt.Errorf("recordedEvent: %w", err)
		}
		err = publisher.Publish(rec, true)
		if err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return nil
}
