package main

import (
	"bytes"
	"fmt"
	"lunadb/internal/protocol"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"go.mongodb.org/mongo-driver/bson"
)

// getCompleter returns the readline.AutoCompleter based on the user's authentication status and transaction state.
func (c *cli) getCompleter() readline.AutoCompleter {
	if !c.isAuthenticated {
		return readline.NewPrefixCompleter(
			readline.PcItem("login"),
			readline.PcItem("help"),
			readline.PcItem("exit"),
			readline.PcItem("clear"),
		)
	}

	// Comandos base disponibles siempre que se está autenticado
	baseItems := []readline.PrefixCompleterInterface{
		readline.PcItem("user",
			readline.PcItem("create"),
			readline.PcItem("update"),
			readline.PcItem("delete"),
		),
		readline.PcItem("update", readline.PcItem("password")),
		readline.PcItem("backup"),
		readline.PcItem("restore"),
		readline.PcItem("collection",
			readline.PcItem("create"),
			readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
			readline.PcItem("list"),
			readline.PcItem("index",
				readline.PcItem("create", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("list", readline.PcItemDynamic(c.fetchCollectionNames)),
			),
			readline.PcItem("item",
				readline.PcItem("get", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("set", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("update", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("list", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("set many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
				readline.PcItem("delete many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
				readline.PcItem("update many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
			),
			readline.PcItem("query", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
			readline.PcItem("update where", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
			readline.PcItem("delete where", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
		),
		readline.PcItem("clear"),
		readline.PcItem("help"),
		readline.PcItem("exit"),
	}

	// Lógica condicional: mostrar comandos de transacción según el estado actual
	if c.inTransaction {
		baseItems = append(baseItems,
			readline.PcItem("commit"),
			readline.PcItem("rollback"),
		)
	} else {
		baseItems = append(baseItems,
			readline.PcItem("begin"),
		)
	}

	return readline.NewPrefixCompleter(baseItems...)
}

// fetchCollectionNames dynamically fetches a list of collection names from the server for autocompletion.
func (c *cli) fetchCollectionNames(line string) []string {
	if time.Since(c.colCacheTime) < 5*time.Second && c.colCache != nil {
		return filterSuggestions(c.colCache, line)
	}

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		var cmdBuf bytes.Buffer
		if err := protocol.WriteCollectionListCommand(&cmdBuf); err != nil {
			continue
		}
		if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
			continue
		}

		statusByte := make([]byte, 1)
		if _, err := c.conn.Read(statusByte); err != nil {
			continue
		}
		status := protocol.ResponseStatus(statusByte[0])

		if _, err := protocol.ReadString(c.conn); err != nil {
			continue
		}

		dataBytes, err := protocol.ReadBytes(c.conn)
		if err != nil || status != protocol.StatusOk {
			continue
		}

		// Leer el BSON envuelto {"list": [...]}
		var resp struct {
			List []string `bson:"list"`
		}
		if bson.Unmarshal(dataBytes, &resp) != nil {
			continue
		}

		c.colCache = resp.List
		c.colCacheTime = time.Now()

		return filterSuggestions(resp.List, line)
	}
	fmt.Fprintln(os.Stderr, colorErr("Warning: Could not fetch collection names for autocompletion after %d attempts.", maxRetries))
	return nil
}

// filterSuggestions es un helper para filtrar la lista según lo que el usuario ha escrito
func filterSuggestions(items []string, line string) []string {
	var prefix string
	if !strings.HasSuffix(line, " ") {
		parts := strings.Fields(line)
		if len(parts) > 0 {
			prefix = parts[len(parts)-1]
		}
	}

	var suggestions []string
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			suggestions = append(suggestions, item)
		}
	}
	return suggestions
}

// fetchJSONFileNames dynamically fetches a list of JSON file names from the 'json' directory.
func (c *cli) fetchJSONFileNames(line string) []string {
	const jsonDir = "json"
	files, err := os.ReadDir(jsonDir)

	if err != nil {
		return nil
	}

	var suggestions []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			suggestions = append(suggestions, file.Name())
		}
	}
	return filterSuggestions(suggestions, line)
}
