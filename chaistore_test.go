package chaistore

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/chaisql/chai"
)

func createDBwithSessionTable(db *chai.DB) error {
	return db.Exec(`
		CREATE TABLE sessions (
			token  TEXT      PRIMARY KEY,
			data   BLOB      NOT NULL,
			expiry TIMESTAMP NOT NULL
		);

		CREATE INDEX idx_sessions_expiry ON sessions(expiry);
	`)
}

func removeDBfile(path string) error {
	fileinfo, _ := os.Stat(path)
	if fileinfo != nil {
		err := os.RemoveAll(path)
		if err != err {
			return err
		}
	}
	return nil
}

func TestFind(t *testing.T) {
	path := "./testchai"

	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}

	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(path)
	defer db.Close()

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("DELETE FROM sessions")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec("INSERT INTO sessions (token, data, expiry) VALUES ('session_token', 'ZW5jb2RlZF9kYXRh', ?)", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 0)

	b, found, err := p.Find("session_token")
	if err != nil {
		t.Fatal(err)
	}
	if found != true {
		t.Fatalf("got %v: expected %v", found, true)
	}
	if bytes.Equal(b, []byte("encoded_data")) == false {
		t.Fatalf("got %v: expected %v", b, []byte("encoded_data"))
	}
}
func TestFindMissing(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("DELETE FROM sessions")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec("INSERT INTO sessions VALUES('session_token', 'ZW5jb2RlZF9kYXRh', ?)", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 0)

	_, found, err := p.Find("missing_session_token")
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}
	if found != false {
		t.Fatalf("got %v: expected %v", found, false)
	}
}

func TestSaveNew(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	defer db.Close()

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("DELETE FROM sessions")
	if err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 0)

	err = p.Commit("session_token", []byte("encoded_data"), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	row, err := db.QueryRow("SELECT data FROM sessions WHERE token = 'session_token'")
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	err = row.Scan(&data)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(data, []byte("encoded_data")) == false {
		t.Fatalf("got %v: expected %v", data, []byte("encoded_data"))
	}
}

func TestSaveUpdated(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	defer db.Close()

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}

	err = db.Exec("DELETE FROM sessions")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec("INSERT INTO sessions VALUES('session_token', 'ZW5jb2RlZF9kYXRh', ?)", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 0)

	err = p.Commit("session_token", []byte("new_encoded_data"), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	row, err := db.QueryRow("SELECT data FROM sessions WHERE token = 'session_token'")
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	err = row.Scan(&data)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(data, []byte("new_encoded_data")) == false {
		t.Fatalf("got %v: expected %v", data, []byte("new_encoded_data"))
	}
}

func TestExpiry(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(path)

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("DELETE FROM sessions")
	if err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 0)
	fmt.Print()
	err = p.Commit("session_token", []byte("encoded_data"), time.Now().Add(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	_, found, _ := p.Find("session_token")
	if found != true {
		t.Fatalf("got %v: expected %v", found, true)
	}

	time.Sleep(100 * time.Millisecond)
	_, found, _ = p.Find("session_token")
	if found != false {
		t.Fatalf("got %v: expected %v", found, false)
	}
}

func TestCleanup(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(path)

	if err := createDBwithSessionTable(db); err != nil {
		t.Fatal(err)
	}

	p := NewWithCleanupInterval(db, 200*time.Millisecond)
	defer p.StopCleanup()

	err = p.Commit("session_token", []byte("encoded_data"), time.Now().Add(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	row, err := db.QueryRow("SELECT COUNT(*) FROM sessions WHERE token = 'session_token'")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("got %d: expected %d", count, 1)
	}

	time.Sleep(300 * time.Millisecond)
	row, err = db.QueryRow("SELECT COUNT(*) FROM sessions WHERE token = 'session_token'")
	if err != nil {
		t.Fatal(err)
	}
	err = row.Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("got %d: expected %d", count, 0)
	}
}

func TestStopNilCleanup(t *testing.T) {
	path := "./testchai"
	if err := removeDBfile(path); err != nil {
		t.Fatal(err)
	}
	db, err := chai.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(path)

	p := NewWithCleanupInterval(db, 0)
	time.Sleep(100 * time.Millisecond)
	// A send to a nil channel will block forever
	p.StopCleanup()
}
