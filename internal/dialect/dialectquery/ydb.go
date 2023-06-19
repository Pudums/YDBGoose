package dialectquery

import (
	"fmt"
)

type YDB struct{}

func (m YDB) CreateTable(tableName string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			hash Uint64,
			version_id Uint64,
			is_applied Bool,
			tstamp Datetime,
			PRIMARY KEY(hash, version_id)
		);`, tableName)
}

func (m YDB) InsertVersion(tableName string) string {
	return fmt.Sprintf(`
	UPSERT INTO %s (
		hash, 
		version_id, 
		is_applied, 
		tstamp
	) VALUES (
		Digest::IntHash64(CAST($1 AS Uint64)), 
		CAST($1 AS Uint64), 
		$2, 
		CurrentUtcDatetime()
	);`, tableName)
}

func (m YDB) GetMigrationByVersion(tableName string) string {
	return fmt.Sprintf(`
		SELECT tstamp, is_applied 
		FROM %s 
		WHERE 
		    hash = Digest::IntHash64(CAST($1 AS Uint64)) 
		AND 
		    version_id = $1 
		ORDER BY tstamp DESC LIMIT 1;`, tableName)
}

func (m YDB) DeleteVersion(tableName string) string {
	return fmt.Sprintf(`
		DELETE FROM %s 
		WHERE 
	    	hash = Digest::IntHash64(CAST($1 AS Uint64)) 
		AND 
		    version_id = $1;`, tableName)
}

func (m *YDB) ListMigrations(tableName string) string {
	return fmt.Sprintf(`
		SELECT version_id, is_applied
		FROM %s
		ORDER BY
			id
		DESC;`, tableName)
}
