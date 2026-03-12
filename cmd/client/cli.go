package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
)

var ErrExit = errors.New("exit requested by user")

type command struct {
	help     string
	handler  func(c *cli, args string) error
	category string
}

type cli struct {
	conn              net.Conn
	rl                *readline.Instance
	rlConfig          *readline.Config
	isAuthenticated   bool
	currentUser       string
	commands          map[string]command
	multiWordCommands []string
	connMutex         sync.Mutex
	inTransaction     bool
	// Nuevos campos para el caché de autocompletado
	colCache     []string
	colCacheTime time.Time
}

// newCLI creates a new command-line interface instance.
func newCLI(conn net.Conn) *cli {
	c := &cli{
		conn: conn,
	}
	c.commands = c.getCommands()

	var mwCmds []string
	for cmd := range c.commands {
		if strings.Contains(cmd, " ") {
			mwCmds = append(mwCmds, cmd)
		}
	}
	sort.Slice(mwCmds, func(i, j int) bool {
		return len(mwCmds[i]) > len(mwCmds[j])
	})
	c.multiWordCommands = mwCmds

	return c
}

// run starts the main CLI loop and handles initial login.
func (c *cli) run(user, pass *string) error {
	c.rlConfig = &readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/readline_history.tmp",
		AutoComplete:    c.getCompleter(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	}

	var err error
	c.rl, err = readline.NewEx(c.rlConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer c.rl.Close()

	if *user != "" && *pass != "" {
		fmt.Println(colorInfo("Attempting automatic login for user ", *user))
		if err := c.handleLogin(fmt.Sprintf("%s %s", *user, *pass)); err != nil {
			fmt.Println(colorErr("Automatic login failed. Please login manually."))
		}
	}

	if !c.isAuthenticated {
		fmt.Println(colorInfo("Please login using: login <username> <password>"))
	}

	return c.mainLoop()
}

// mainLoop is the core loop that reads user input and executes commands.
func (c *cli) mainLoop() error {
	for {
		var prompt string
		if c.isAuthenticated && c.currentUser != "" {
			prompt = c.currentUser
		}
		if c.inTransaction {
			prompt += "[TX]"
		}
		prompt += "> "

		c.rl.SetPrompt(colorPrompt(prompt))

		input, err := c.rl.Readline()
		if err != nil {
			// MEJORA: Prevenir transacciones zombis al salir
			if errors.Is(err, readline.ErrInterrupt) || errors.Is(err, io.EOF) {
				if c.inTransaction {
					fmt.Println(colorErr("\n[!] Active transaction detected. Rolling back before exit..."))
					c.handleRollback("")
				}
				if len(input) == 0 || errors.Is(err, io.EOF) {
					break
				}
				continue
			}
			return err
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		cmd, args := c.getCommandAndRawArgs(input)
		handler, found := c.commands[cmd]
		if !found {
			fmt.Println(colorErr("Error: Unknown command. Type 'help' for commands: ", cmd))
			continue
		}

		if !c.isAuthenticated && cmd != "login" && cmd != "help" && cmd != "clear" && cmd != "exit" {
			fmt.Println(colorErr("Error: You must log in first. Use: login <username> <password>"))
			continue
		}

		startTime := time.Now()

		c.connMutex.Lock()
		err = handler.handler(c, args)
		c.connMutex.Unlock()

		if err != nil {
			// 1. Caso en el que el usuario escribió 'exit'
			if errors.Is(err, ErrExit) {
				if c.inTransaction {
					fmt.Println(colorErr("\n[!] Active transaction detected. Rolling back before exit..."))
					c.handleRollback("")
				}
				break
			}

			// 2. Caso en el que la conexión de red murió (servidor apagado/caído)
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "connection reset") {
				fmt.Println(colorErr("\n[!] Connection to the server was lost (Server might be down). Exiting client safely..."))
				break // Salimos inmediatamente sin intentar hacer rollback en la red muerta
			}

			// 3. Cualquier otro error normal de comandos
			fmt.Println(colorErr("Command failed: ", err))
		}
		duration := time.Since(startTime)
		if cmd != "clear" && cmd != "help" && cmd != "exit" {
			fmt.Println(colorInfo("Request time: ", duration.Round(time.Millisecond)))
		}
	}
	fmt.Println(colorInfo("\nExiting client. Goodbye!"))
	return nil
}
