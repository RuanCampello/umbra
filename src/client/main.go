package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@localhost:5432/testing_db")
	if err != nil {
		log.Fatalf("Unable to connect to postgres database: %v", err)
	}

	client := &Client{
		ctx: ctx,
		db:  conn,
	}

	err = client.seedDatabase()
	if err != nil {
		log.Fatalf("Failed to seed the database: %v", err)
	}

	defer conn.Close(ctx)
}
