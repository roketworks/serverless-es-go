package eventstore

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type CheckpointConfig struct {
	ConnectionString string
	ProjectionName   string
}

func SaveCheckpoint(cfg *CheckpointConfig, position int, timestamp int64) error {
	db, err := sql.Open("postgres", cfg.ConnectionString)
	if err != nil {
		return err
	}

	if _, err = db.Exec(schema); err != nil {
		return err
	}

	db.Begin()
	if _, err = db.Exec(update, cfg.ProjectionName, position, timestamp); err != nil {
		db.Close()
		return err
	}
	db.Close()

	return nil
}

const schema = `
	CREATE TABLE IF NOT EXISTS checkpoints
	(
		name        varchar(50) unique,
		position    int,
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
