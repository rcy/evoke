package evoke

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

type Event struct {
	EventID       int       `db:"event_id"`
	CreatedAt     time.Time `db:"created_at"`
	AggregateID   string    `db:"aggregate_id"`
	AggregateType string    `db:"aggregate_type"`
	EventType     string    `db:"event_type"`
	EventData     []byte    `db:"event_data"`
}

type SagaInstance struct {
	SagaInstanceID int       `db:"saga_instance_id"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
	EventID        int       `db:"event_id"`
	SagaName       string    `db:"saga_name"`
	Status         string    `db:"status"`
	LastError      string    `db:"lastError"`
}

type SagaHandler struct {
	Name        string
	HandlerFunc HandlerFunc
}

type Service struct {
	db     *sqlx.DB
	Config *Config

	// handlers that run inside the event insertion transaction (necessary projections)
	handlers map[string][]*HandlerFunc
	// handlers that run outside the event insertion transiaction (ie sagas)
	xhandlers map[string][]*SagaHandler
}

type Config struct {
	DBFile string
}

func NewStore(config Config) (*Service, error) {
	err := os.MkdirAll(filepath.Dir(config.DBFile), 0755)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", config.DBFile)
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
			event_id integer primary key autoincrement,
                        created_at timestamp not null default current_timestamp,
                        aggregate_type text not null,
                        aggregate_id text not null,
                        event_type text not null,
                        event_data text not null
		);
	`); err != nil {
		return nil, fmt.Errorf("failed to create events table: %w", err)
	}

	if _, err := db.Exec(`
		create table if not exists saga_instances (
                        saga_instance_id integer primary key autoincrement,
                        created_at timestamp not null default current_timestamp,
                        updated_at timestamp not null default current_timestamp,
                        event_id text not null,
                        saga_name text not null,
                        status text not null,
                        last_error text
		);
	`); err != nil {
		return nil, fmt.Errorf("failed to create saga_instances table: %w", err)
	}

	sqlxDB := sqlx.NewDb(db, "sqlite3")

	return &Service{
		db:        sqlxDB,
		Config:    &config,
		handlers:  make(map[string][]*HandlerFunc),
		xhandlers: make(map[string][]*SagaHandler),
	}, nil
}

func (s *Service) Close() error {
	return s.db.Close()
}

// Event payloads must implement this interface
type EventDefinition interface {
	EventType() string
	Aggregate() string
}

type Inserter interface {
	Insert(aggregateID string, def EventDefinition) error
	GetAggregateID(prefix string) (string, error)
}

type ExecGetter interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Get(dest interface{}, query string, args ...interface{}) error
}

type HandlerFunc func(event Event, replay bool) error

// Subscribe to event and have handler run inside event insert transaction
func (s *Service) SubscribeSync(payload EventDefinition, handler HandlerFunc) {
	key := payload.EventType()
	s.handlers[key] = append(s.handlers[key], &handler)
}

// Subscribe to event and have handler run outside event insert transaction
func (s *Service) Subscribe(name string, payload EventDefinition, handler HandlerFunc) {
	key := payload.EventType()
	s.xhandlers[key] = append(s.xhandlers[key], &SagaHandler{Name: name, HandlerFunc: handler})
}

func (s *Service) GetAggregateIDs(prefix string) ([]string, error) {
	var aggIDs []string
	query := fmt.Sprintf("%s%%", prefix)
	err := s.db.Select(&aggIDs, `select distinct aggregate_id from events where aggregate_id like ?`, query)
	if err != nil {
		return nil, fmt.Errorf("Select: %w", err)
	}
	return aggIDs, nil
}

func (s *Service) GetAggregateID(prefix string) (string, error) {
	aggIDs, err := s.GetAggregateIDs(strings.ToLower(prefix))
	if err != nil {
		return "", fmt.Errorf("GetAggregateID: %w", err)
	}

	if len(aggIDs) == 0 {
		return "", fmt.Errorf("ID not found")
	}
	if len(aggIDs) > 1 {
		return "", fmt.Errorf("ID is ambiguous: %s", aggIDs)
	}
	return aggIDs[0], nil
}

func (s *Service) LoadAggregateEvents(aggregateID string) ([]Event, error) {
	var events []Event
	err := s.db.Select(&events, `select * from events where aggregate_id = ? order by event_id asc`, aggregateID)
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) LoadAllEvents(reverse bool) ([]Event, error) {
	var events []Event

	var order string
	if reverse {
		order = "desc"
	} else {
		order = "asc"
	}
	err := s.db.Select(&events, fmt.Sprintf(`select * from events order by event_id %s`, order))
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) Insert(aggregateID string, payload EventDefinition) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("Begin: %w", err)
	}
	defer tx.Rollback()

	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	var event Event
	err = tx.Get(&event, `insert into events(aggregate_id, aggregate_type, event_type, event_data) values (?,?,?,?) returning *`,
		aggregateID,
		payload.Aggregate(),
		payload.EventType(),
		string(bytes)) // Q: why string bytes? sqlite thing?
	if err != nil {
		return fmt.Errorf("db.Exec: %w", err)
	}

	err = s.runEventHandlers(event, false)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Commit: %w", err)
	}

	err = s.xrunEventHandlers(event, false)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) runEventHandlers(event Event, replay bool) error {
	if handlers, ok := s.handlers[event.EventType]; ok {
		//inserter := InsertTxWrapper{e: e, s: s}
		for _, handler := range handlers {
			err := (*handler)(event, replay)
			if err != nil {
				return fmt.Errorf("handler for %s failed: %w", event.EventType, err)
			}
		}
	}

	return nil
}

func (s *Service) xrunEventHandlers(event Event, replay bool) error {
	if handlers, ok := s.xhandlers[event.EventType]; ok {
		for _, handler := range handlers {
			var sagaInstanceID int
			err := s.db.Get(&sagaInstanceID, `insert into saga_instances(event_id, saga_name, status) values(?,?,?) returning saga_instance_id`, event.EventID, handler.Name, "running")
			if err != nil {
				return fmt.Errorf("error creating saga instance: %w", err)
			}
			err = handler.HandlerFunc(event, replay)
			if err != nil {
				fmt.Fprintf(os.Stderr, "SAGA FAIL %v | %v: %s\n", event, handler, err)
				_, err = s.db.Exec(`update saga_instances set status = 'error', last_error = ?, updated_at = ? where saga_instance_id = ?`,
					err.Error(), time.Now(), sagaInstanceID)
				if err != nil {
					return fmt.Errorf("error updating saga instance with error: %w", err)
				}
			} else {
				_, err = s.db.Exec(`update saga_instances set status = 'completed', updated_at = ? where saga_instance_id = ?`, time.Now(), sagaInstanceID)
				if err != nil {
					return fmt.Errorf("error updating saga instance as completed: %w", err)
				}
			}
		}
	}

	return nil
}

func (s *Service) Replay() error {
	events := []Event{}
	if err := s.db.Select(&events, `select * from events order by event_id asc`); err != nil {
		return err
	}

	for i, event := range events {
		err := s.runEventHandlers(event, true)
		if err != nil {
			return fmt.Errorf("replay failed at event %d (%s): %w", i, event.EventType, err)
		}
	}
	return nil
}

func UnmarshalPayload[T any](event Event) (T, error) {
	var payload T
	err := json.Unmarshal([]byte(event.EventData), &payload)
	return payload, err
}

func ID() string {
	src := make([]byte, 20)
	_, _ = rand.Read(src)
	return fmt.Sprintf("%x", src)
}
