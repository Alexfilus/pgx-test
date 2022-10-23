package main

import (
	"context"
	"fmt"
	"log"

	pgxpoolV4 "github.com/jackc/pgx/v4/pgxpool"
	pgxpoolV5 "github.com/jackc/pgx/v5/pgxpool"
)

const query = "select id from pgx_test.data"

type Config struct {
	Host     string `env:"PG_HOST"`
	Port     string `env:"PG_PORT"`
	User     string `env:"PG_USER"`
	Password string `env:"PG_PASS"`
	DbName   string `env:"PG_DBNAME"`
}

func main() {
	conf := Config{
		Host:   "127.0.0.1",
		Port:   "6432",
		DbName: "habrdb",
		// User:     "habrpguser",
		// Password: "pgpwd4habr",
		User:     "pgx_test",
		Password: "qwerty",
	}

	dbV5, err := NewV5(conf)
	if err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()

	dbV4, err := NewV4(conf)
	if err != nil {
		log.Fatalln(err)
	}
	rowsV4, err := dbV4.Query(ctx, query)
	if err != nil {
		log.Fatalln(err)
	}
	if rowsV4.Err() != nil {
		log.Fatalln(err)
	}
	defer rowsV4.Close()
	for rowsV4.Next() {
		var id int
		err = rowsV4.Scan(&id)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(id)
	}

	rowsV5, err := dbV5.Query(ctx, query)
	if err != nil {
		log.Fatalln(err)
	}
	if rowsV5.Err() != nil {
		log.Fatalln(err)
	}
	defer rowsV5.Close()
	for rowsV5.Next() {
		var id int
		err = rowsV5.Scan(&id)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(id)
	}
}

func NewV4(cfg Config) (*pgxpoolV4.Pool, error) {
	ctx := context.Background()
	url := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DbName)
	config, err := pgxpoolV4.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	db, err := pgxpoolV4.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

func NewV5(cfg Config) (*pgxpoolV5.Pool, error) {
	ctx := context.Background()
	url := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DbName)
	config, err := pgxpoolV5.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	db, err := pgxpoolV5.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(ctx); err != nil {
		return nil, err
	}

	return db, nil
}
