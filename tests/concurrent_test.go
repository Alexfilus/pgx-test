package tests

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *TestSuite) Test_ConcurrentQuery() {
	tests := []struct {
		name  string
		query string
		count int
		f     func(rows pgx.Rows, ttRes any) (any, error)
	}{
		{
			name:  "one number",
			query: `select $1::int`,
			count: 1000,
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectOneRow[int](rows, pgx.RowTo[int])
			},
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		wg := sync.WaitGroup{}
		wg.Add(tt.count)
		for i := 0; i < tt.count; i++ {
			go func(i int) {
				defer wg.Done()
				rows, err := s.pgxPool.Query(ctx, tt.query, i)
				s.Require().NoError(err)
				res, err := tt.f(rows, i)
				s.Require().NoError(err)
				s.Require().Equal(i, res)
			}(i)
		}
		wg.Wait()
	}
}

func (s *TestSuite) Test_ConcurrentExec() {
	ctx := context.Background()
	_, err := s.pgxPool.Exec(ctx, `create table if not exists test (id int primary key, str text, dur_str interval, dur_time interval)`)
	s.Require().NoError(err)

	conn, err := s.pgxPool.Acquire(ctx)
	s.Require().NoError(err)
	s.T().Log(conn.Conn().TypeMap())
	conn.Release()

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := i
			str := strconv.Itoa(i)
			durStr := strconv.Itoa(i) + " days"
			durTime := time.Duration(i) * time.Second
			s.T().Log(id, str, durStr, durTime)
			// _, err = s.pgxPool.Exec(ctx, `
			// insert into test (id, str, dur_str, dur_time)
			// values ($1, $2, $3, $4)`, id, str, durStr, durTime)

			_, err = s.pgxPool.Exec(ctx, `
insert into test (id) values ($1)`, id)
			s.Require().NoError(err)

			_, err = s.pgxPool.Exec(ctx, `
update test set str = $2 where id = $1`, id, str)
			s.Require().NoError(err)

			_, err = s.pgxPool.Exec(ctx, `
update test set dur_str = $2 where id = $1`, id, durStr)
			s.Require().NoError(err)

			_, err = s.pgxPool.Exec(ctx, `
update test set dur_time = $2 where id = $1`, id, durTime)
			s.Require().NoError(err)

		}(i)
	}
	wg.Wait()

	conn, err = s.pgxPool.Acquire(ctx)
	s.Require().NoError(err)
	s.T().Log(conn.Conn().TypeMap())
	conn.Release()
}
