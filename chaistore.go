package chaistore

import (
	"log"
	"time"

	"github.com/chaisql/chai"
)

// ChaiStore represents the session store.
type ChaiStore struct {
	db          *chai.DB
	stopCleanup chan bool
}

// New returns a new ChaiStore instance, with a background cleanup goroutine
// that runs every 5 minutes to remove expired session data.
func New(db *chai.DB) *ChaiStore {
	return NewWithCleanupInterval(db, 5*time.Minute)
}

// NewWithCleanupInterval returns a new ChaiStore instance. The cleanupInterval
// parameter controls how frequently expired session data is removed by the
// background cleanup goroutine. Setting it to 0 prevents the cleanup goroutine
// from running (i.e. expired sessions will not be removed).
func NewWithCleanupInterval(db *chai.DB, cleanupInterval time.Duration) *ChaiStore {
	p := &ChaiStore{db: db}
	if cleanupInterval > 0 {
		go p.startCleanup(cleanupInterval)
	}
	return p
}

// Find returns the data for a given session token from the ChaiStore instance.
// If the session token is not found or is expired, the returned exists flag will
// be set to false.
func (p *ChaiStore) Find(token string) (b []byte, exists bool, err error) {
	row, err := p.db.QueryRow("SELECT data FROM sessions WHERE token = ? AND ? < expiry", token, time.Now())
	if chai.IsNotFoundError(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return b, true, row.Scan(&b)
}

// Commit adds a session token and data to the ChaiStore instance with the
// given expiry time. If the session token already exists, then the data and expiry
// time are updated.
func (p *ChaiStore) Commit(token string, b []byte, expiry time.Time) error {
	return p.db.Exec("INSERT INTO sessions (token, data, expiry) VALUES (?, ?, ?) ON CONFLICT REPLACE", token, b, expiry.UTC())
}

// Delete removes a session token and corresponding data from the ChaiStore
// instance.
func (p *ChaiStore) Delete(token string) error {
	return p.db.Exec("DELETE FROM sessions WHERE token = ?", token)
}

// All returns a map containing the token and data for all active (i.e.
// not expired) sessions in the ChaiStore instance.
func (p *ChaiStore) All() (map[string][]byte, error) {
	rows, err := p.db.Query("SELECT token, data FROM sessions WHERE ? < expiry", time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make(map[string][]byte)

	err = rows.Iterate(func(row *chai.Row) error {
		var (
			token string
			data  []byte
		)

		err = row.Scan(&token, &data)
		if err != nil {
			return err
		}

		sessions[token] = data
		return nil
	})

	return sessions, err
}

func (p *ChaiStore) startCleanup(interval time.Duration) {
	p.stopCleanup = make(chan bool)
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			err := p.deleteExpired()
			if err != nil {
				log.Println(err)
			}
		case <-p.stopCleanup:
			ticker.Stop()
			return
		}
	}
}

// StopCleanup terminates the background cleanup goroutine for the ChaiStore
// instance. It's rare to terminate this; generally ChaiStore instances and
// their cleanup goroutines are intended to be long-lived and run for the lifetime
// of your application.
//
// There may be occasions though when your use of the ChaiStore is transient.
// An example is creating a new ChaiStore instance in a test function. In this
// scenario, the cleanup goroutine (which will run forever) will prevent the
// ChaiStore object from being garbage collected even after the test function
// has finished. You can prevent this by manually calling StopCleanup.
func (p *ChaiStore) StopCleanup() {
	if p.stopCleanup != nil {
		p.stopCleanup <- true
	}
}

func (p *ChaiStore) deleteExpired() error {
	return p.db.Exec("DELETE FROM sessions WHERE expiry < ?", time.Now())
}
