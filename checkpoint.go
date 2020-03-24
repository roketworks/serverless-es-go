package esgo

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type PostgresCheckpoint struct {
	ConnectionString string
	ProjectionName   string
}

// Save checkpoint position to postgres database.
// Will also create/migrate schema if doesn't exist.
func (checkpoint *PostgresCheckpoint) Save(position int64, timestamp int64) error {
	db, err := sql.Open("postgres", checkpoint.ConnectionString)
	if err != nil {
		return err
	}

	if _, err = db.Exec(schema); err != nil {
		return err
	}

	var tx *sql.Tx
	if tx, err = db.Begin(); err != nil {
		return err
	}
	if _, err = tx.Exec(update, checkpoint.ProjectionName, position, timestamp); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}

	return nil
}

const schema = `
	CREATE TABLE IF NOT EXISTS checkpoints
	(
		name        varchar(50) unique,
		position    bigint,
		timestamp 	bigint
	);
`

const update = `
	INSERT INTO checkpoints (name, position, timestamp)
	VALUES ($1, $2, $3)
	ON CONFLICT(name)
	DO
	  UPDATE SET
		position = $2,
		timestamp = $3
`
