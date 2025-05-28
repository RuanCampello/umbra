package tui

import (
	"belfry/db"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type model struct {
	client *db.Client
	screen screen
	width  int
	height int
}

type screen = int8

const (
	sidebar screen = iota
	main
)

func NewModel(client *db.Client) model {
	return model{client: client, screen: 0}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	}

	return m, nil
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) View() string {
	sidebarWidth := m.width / 4
	mainScreenWidth := m.width - sidebarWidth

	sidebarStyle := lipgloss.NewStyle().Width(sidebarWidth).Height(m.height).Border(lipgloss.RoundedBorder()).BorderRight(true).Padding(2)
	mainStyle := lipgloss.NewStyle().Width(mainScreenWidth).Height(m.height).Border(lipgloss.RoundedBorder()).BorderLeft(true).Padding(2)

	sidebar := sidebarStyle.Render("Sidebar \n Content")
	main := mainStyle.Render("Main page \n Content")

	return lipgloss.JoinHorizontal(lipgloss.Top, sidebar, main)
}
