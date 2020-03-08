package eventstore

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type CheckpointConfig struct {
	connectionString string
	projectionName   string
}

func SaveCheckpoint(cfg *CheckpointConfig, position int) error {
	db, err := sql.Open("postgres", cfg.connectionString)
	if err != nil {
		return err
	}

	_, err = db.Exec(schema)
	if err != nil {
		return err
	}

	db.Begin()
	_, err = db.Exec(update, cfg.projectionName, position)
	if err != nil {
		db.Close()
		return err
	}
	db.Close()

	return nil
}

const schema = `
	CREATE TABLE IF NOT EXISTS checkpoints
	(
		name        varchar(50),
		position    int
	);
`

const update = `
	INSERT INTO checkpoints (name, position)
	VALUES (?, ?)
	ON CONFLICT(name)
	DO
	  UPDATE SET
		position = ?
`
