package db

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Client struct {
	ctx context.Context
	db  *pgx.Conn
}

func NewClient(ctx context.Context, db *pgx.Conn) Client {
	return Client{ctx, db}
}

func (c *Client) SeedDatabase() error {
	sql, err := os.ReadFile("seed.sql")
	if err != nil {
		return fmt.Errorf("Failed to parse seed.sql: %v", err)
	}

	statements := strings.Split(string(sql), ";")
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		_, err := c.db.Exec(c.ctx, statement)
		if err != nil {
			return fmt.Errorf("Failed to execute statement: %s \n %v", statement, err)
		}
	}

	println("Database seeded!")
	return nil
}
