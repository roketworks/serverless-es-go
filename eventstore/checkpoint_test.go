package eventstore

import (
	"testing"

	"database/sql"
	_ "github.com/lib/pq"

	"github.com/roketworks/serverless-es-go/test"
	"github.com/stretchr/testify/assert"
)

var connectionString = test.Configuration.Postgres.ConnectionString

func TestSaveNewCheckpoint(t *testing.T) {
	cfg := CheckpointConfig{ConnectionString: connectionString, ProjectionName: "test-projection"}
	err := SaveCheckpoint(&cfg, 1)
	assert.Nil(t, err)
	verifyCheckpoint(t, "test-projection", 1)
}

func verifyCheckpoint(t *testing.T, name string, position int) {
	db, err := sql.Open("postgres", connectionString)
	assert.Nil(t, err)

	var projectionName string
	var checkpointPosition int

	err = db.QueryRow("SELECT * FROM checkpoints WHERE name = $1", name).Scan(&projectionName, &checkpointPosition)
	assert.Nil(t, err)

	assert.Equal(t, name, projectionName)
	assert.Equal(t, position, checkpointPosition)
}
