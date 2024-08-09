package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/suite"
)

type TestSuite struct {
	suite.Suite
	dockerPool        *dockertest.Pool
	postgresContainer *dockertest.Resource
	pgxPool           *pgxpool.Pool
}

func TestPgx(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (s *TestSuite) SetupSuite() {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		s.T().Fatalf("error creating testing env: %v", err)
	}

	resource, err := dockerPool.Run(
		"postgres",
		"15",
		[]string{"POSTGRES_PASSWORD=secret", "POSTGRES_USER=test", "POSTGRES_DB=mpm"})
	if err != nil {
		s.T().Fatalf("couldn't start test postgresContainer container: %v", err)
	}

	ctx := context.Background()
	host := os.Getenv("APP_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := resource.GetPort("5432/tcp")

	if err = dockerPool.Retry(func() error {
		cfg, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://test:secret@%s:%s/%s?sslmode=disable", host, port, "mpm"))
		if err != nil {
			return err
		}
		cfg.MinConns = 10
		cfg.MaxConns = 20
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		s.pgxPool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return err
		}
		return s.pgxPool.Ping(ctx)
	}); err != nil {
		s.T().Fatalf("couldn't connect to postgresContainer: %v", err)
	}

	if err := resource.Expire(30); err != nil {
		s.T().Fatalf("couldn't set expire time for postgresContainer container: %v", err)
	}

	s.dockerPool = dockerPool
	s.postgresContainer = resource

	_, err = s.pgxPool.Exec(ctx, `create table if not exists test (id int primary key, str text, dur_str interval, dur_time interval)`)
	s.Require().NoError(err)

}

func (s *TestSuite) TearDownSuite() {
	if err := s.dockerPool.Purge(s.postgresContainer); err != nil {
		s.T().Fatalf("couldn't kill test postgresContainer container: %v", err)
	}
}

func (s *TestSuite) TearDownTest() {
	_, err := s.pgxPool.Exec(context.Background(), "truncate table test")
	s.Require().NoError(err)
}
