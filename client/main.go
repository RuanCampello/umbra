package main

import (
	"context"
	"log"

	"belfry/db"
	"belfry/tui"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@localhost:5432/testing_db")
	if err != nil {
		log.Fatalf("Unable to connect to postgres database: %v", err)
	}

	client := db.NewClient(ctx,
		conn,
	)

	err = client.SeedDatabase()
	if err != nil {
		log.Fatalf("Failed to seed the database: %v", err)
	}

	model := tui.NewModel(&client)

	p := tea.NewProgram(model, tea.WithAltScreen())
	_, err = p.Run()
	if err != nil {
		log.Fatalf("It's over, mate")
	}

	defer conn.Close(ctx)
}
