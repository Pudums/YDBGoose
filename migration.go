package goose

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MigrationRecord struct.
type MigrationRecord struct {
	VersionID int64
	TStamp    time.Time
	IsApplied bool // was this a result of up() or down()
}

// Migration struct.
type Migration struct {
	Version  int64
	Next     int64               // next version, or -1 if none
	Previous int64               // previous version, -1 if none
	Source   string              // path to .sql script
	UpFn     func(*sql.Tx) error // Up go migration function
	DownFn   func(*sql.Tx) error // Down go migration function
}

func (m *Migration) String() string {
	return fmt.Sprintf(m.Source)
}

// Up runs an up migration.
func (m *Migration) Up(db *sql.DB) error {
	return m.run(db, true)
}

// Down runs a down migration.
func (m *Migration) Down(db *sql.DB) error {
	return m.run(db, false)
}

func (m *Migration) run(db *sql.DB, direction bool) error {
	switch filepath.Ext(m.Source) {
	case ".sql":
		if err := runSQLMigration(db, m.Source, m.Version, direction); err != nil {
			return fmt.Errorf("FAIL %v, quitting migration", err)
		}

	case ".go":
		tx, err := db.Begin()
		if err != nil {
			log.Fatal("db.Begin: ", err)
		}

		fn := m.UpFn
		if !direction {
			fn = m.DownFn
		}
		if fn != nil {
			if err := fn(tx); err != nil {
				tx.Rollback()
				log.Fatalf("FAIL %s (%v), quitting migration.", filepath.Base(m.Source), err)
				return err
			}
		}

		if err = FinalizeMigration(tx, direction, m.Version); err != nil {
			log.Fatalf("error finalizing migration %s, quitting. (%v)", filepath.Base(m.Source), err)
		}
	}

	fmt.Println("OK   ", filepath.Base(m.Source))

	return nil
}

// NumericComponent looks for migration scripts with names in the form:
// XXX_descriptivename.ext where XXX specifies the version number
// and ext specifies the type of migration
func NumericComponent(name string) (int64, error) {

	base := filepath.Base(name)

	if ext := filepath.Ext(base); ext != ".go" && ext != ".sql" {
		return 0, errors.New("not a recognized migration file type")
	}

	idx := strings.Index(base, "_")
	if idx < 0 {
		return 0, errors.New("no separator found")
	}

	n, e := strconv.ParseInt(base[:idx], 10, 64)
	if e == nil && n <= 0 {
		return 0, errors.New("migration IDs must be greater than zero")
	}

	return n, e
}

// FinalizeMigration updates the version table for the given migration,
// and finalize the transaction.
func FinalizeMigration(tx *sql.Tx, direction bool, v int64) error {

	// XXX: drop goose_db_version table on some minimum version number?
	stmt := GetDialect().insertVersionSQL()
	if _, err := tx.Exec(stmt, v, direction); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}