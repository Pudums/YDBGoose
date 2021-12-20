package dialectquery

import (
	"database/sql"
	"fmt"
)

type YDB struct{}

func (m YDB) CreateTable(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
      version_id Int64,
      is_applied UInt8,
      tstamp DateTime,
      PRIMARY KEY(version_id)
    ) `, tableName)
}

func (m YDB) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied FROM %s ORDER BY tstamp DESC LIMIT 1", tableName))
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (m YDB) InsertVersion(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied, tstamp) VALUES (?, ?, CurrentUtcDatetime())", tableName)
}

func (m YDB) GetMigrationByVersion(tableName string) string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id = ? ORDER BY tstamp DESC LIMIT 1", tableName)
}

func (m YDB) DeleteVersion(tableName string) string {
	return fmt.Sprintf("ALTER TABLE %s DELETE WHERE version_id = ?", tableName)
}

func (m *YDB) ListMigrations(tableName string) string {
	q := `SELECT version_id, is_applied from %s ORDER BY id DESC`
	return fmt.Sprintf(q, tableName)
}
