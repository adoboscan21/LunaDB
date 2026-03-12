package main

import (
	"bytes"
	"encoding/base64"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"lunadb/internal/protocol"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"go.mongodb.org/mongo-driver/bson"
)

// Color definitions for the interface
var (
	colorOK     = color.New(color.FgGreen, color.Bold).SprintFunc()
	colorErr    = color.New(color.FgRed, color.Bold).SprintFunc()
	colorPrompt = color.New(color.FgMagenta).SprintFunc()
	colorInfo   = color.New(color.FgBlue).SprintFunc()
)

// jsonToBson convierte texto JSON ingresado por el usuario a binario BSON para el servidor.
func jsonToBson(jsonData []byte) ([]byte, error) {
	jsonData = bytes.TrimSpace(jsonData)
	if len(jsonData) == 0 {
		return nil, errors.New("empty json")
	}

	// WORKAROUND BSON: BSON no soporta arrays en la raíz. Si es un array, lo envolvemos en un documento.
	if jsonData[0] == '[' {
		var arr []any
		if err := stdjson.Unmarshal(jsonData, &arr); err != nil {
			return nil, fmt.Errorf("invalid json array: %w", err)
		}
		return bson.Marshal(bson.M{"array": arr})
	}

	// Si es un objeto normal, lo procesamos estándar
	var doc map[string]any
	if err := stdjson.Unmarshal(jsonData, &doc); err != nil {
		return nil, fmt.Errorf("invalid json object: %w", err)
	}
	return bson.Marshal(doc)
}

// bsonToJson convierte binario BSON del servidor a texto JSON formateado para el usuario.
func bsonToJson(bsonData []byte) ([]byte, error) {
	if len(bsonData) == 0 {
		return []byte("{}"), nil
	}
	var doc map[string]any
	if err := bson.Unmarshal(bsonData, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bson: %w", err)
	}
	return stdjson.MarshalIndent(doc, "  ", "  ")
}

// getCommandAndRawArgs parses user input into a command and its arguments.
func (c *cli) getCommandAndRawArgs(input string) (string, string) {
	for _, mwCmd := range c.multiWordCommands {
		if strings.HasPrefix(input, mwCmd+" ") || input == mwCmd {
			return mwCmd, strings.TrimSpace(input[len(mwCmd):])
		}
	}

	parts := strings.SplitN(input, " ", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

// clearScreen clears the terminal screen.
func clearScreen() {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "cls")
	default:
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	_ = cmd.Run()
}

// getStatusString converts a ResponseStatus to a human-readable string.
func getStatusString(s protocol.ResponseStatus) string {
	switch s {
	case protocol.StatusOk:
		return "OK"
	case protocol.StatusNotFound:
		return "NOT_FOUND"
	case protocol.StatusError:
		return "ERROR"
	case protocol.StatusBadCommand:
		return "BAD_COMMAND"
	case protocol.StatusUnauthorized:
		return "UNAUTHORIZED"
	case protocol.StatusBadRequest:
		return "BAD_REQUEST"
	default:
		return "UNKNOWN"
	}
}

// getRawFileOrString extrae los bytes crudos ya sea de un archivo o de un string directo
func (c *cli) getRawFileOrString(payload string) ([]byte, error) {
	if strings.HasSuffix(payload, ".json") {
		if data, err := os.ReadFile(payload); err == nil {
			return data, nil
		}
		filePath := filepath.Join("json", payload)
		if data, err := os.ReadFile(filePath); err == nil {
			return data, nil
		}
		return nil, fmt.Errorf("could not read JSON file '%s'", payload)
	}
	return []byte(payload), nil
}

// getJSONPayload reads JSON data (directly or file), AND converts it to BSON payload.
func (c *cli) getJSONPayload(payload string) ([]byte, error) {
	raw, err := c.getRawFileOrString(payload)
	if err != nil {
		return nil, err
	}
	return jsonToBson(raw)
}

// resolveCollectionName parses the command arguments to extract the collection name.
func (c *cli) resolveCollectionName(args string, commandName string) (string, string, error) {
	args = strings.TrimSpace(args)
	if args == "" {
		usage := fmt.Sprintf("usage: %s <collection_name> [other_args...]", commandName)
		return "", "", errors.New("no collection name provided. " + usage)
	}

	parts := strings.SplitN(args, " ", 2)
	collectionName := parts[0]

	var remainingArgs string
	if len(parts) > 1 {
		remainingArgs = parts[1]
	}

	return collectionName, remainingArgs, nil
}

// readResponse reads a full response from the server and prints it in a formatted way.
func (c *cli) readResponse(lastCmd string) error {
	status, msg, dataBytes, err := c.readRawResponse()
	if err != nil {
		return err
	}

	statusStr := getStatusString(status)
	if status == protocol.StatusOk {
		fmt.Printf("%s %s\n", colorOK("["+statusStr+"]"), msg)
	} else {
		fmt.Printf("%s %s\n", colorErr("["+statusStr+"]"), msg)
	}

	if len(dataBytes) == 0 {
		fmt.Println("---")
		return nil
	}

	// Traducir BSON a JSON para visualización
	jsonBytes, err := bsonToJson(dataBytes)

	switch lastCmd {
	case "collection list", "collection index list", "collection item list", "collection query":
		if err == nil {
			if tblErr := printDynamicTable(jsonBytes); tblErr != nil {
				fmt.Printf("  %s\n%s\n", colorInfo("Data:"), string(jsonBytes))
			}
		} else {
			fmt.Println(colorErr("Could not decode BSON response."))
		}
	default:
		if err == nil {
			fmt.Printf("  %s\n%s\n", colorInfo("Data:"), string(jsonBytes))
		} else {
			if s, ok := tryDecodeBase64(dataBytes); ok {
				fmt.Printf("  %s %s\n", colorInfo("Data (Decoded):"), s)
			} else {
				fmt.Printf("  %s (Raw BSON Output Hidden for Safety)\n", colorInfo("Data:"))
			}
		}
	}
	fmt.Println("---")
	return nil
}

func tryDecodeBase64(data []byte) (string, bool) {
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return "", false
	}
	var prettyJSON bytes.Buffer
	if stdjson.Indent(&prettyJSON, decoded, "  ", "  ") == nil {
		return prettyJSON.String(), true
	}
	return string(decoded), true
}

// printDynamicTable busca el primer array dentro de los datos y lo renderiza como tabla dinámica.
func printDynamicTable(dataBytes []byte) error {
	// Parseamos el documento raíz (objeto)
	var root map[string]any
	if err := stdjson.Unmarshal(dataBytes, &root); err != nil {
		return errors.New("response is not a valid object")
	}

	// Buscamos dinámicamente cuál llave contiene el array ("list", "results", "array", etc.)
	var objectArrayResults []any
	for _, v := range root {
		if arr, ok := v.([]any); ok {
			objectArrayResults = arr
			break
		}
	}

	if len(objectArrayResults) == 0 {
		fmt.Println("(No results or data is not in array format)")
		return nil
	}

	headerSet := make(map[string]bool)
	var parsedDocs []map[string]any

	// Normalizamos los elementos a map[string]any (Soportando primitivos)
	for _, docAny := range objectArrayResults {
		if doc, ok := docAny.(map[string]any); ok {
			// Es un objeto estándar
			parsedDocs = append(parsedDocs, doc)
			for key := range doc {
				headerSet[key] = true
			}
		} else {
			// Es un valor primitivo (Ej: arreglo de strings como listas de índices o colecciones)
			primitiveDoc := map[string]any{"Item": docAny}
			parsedDocs = append(parsedDocs, primitiveDoc)
			headerSet["Item"] = true
		}
	}

	if len(parsedDocs) == 0 {
		fmt.Println("(No valid objects to format into a table)")
		return nil
	}

	headers := make([]string, 0, len(headerSet))
	for key := range headerSet {
		headers = append(headers, key)
	}
	sort.Strings(headers)

	table := tablewriter.NewWriter(os.Stdout)
	table.Header(headers)

	for _, doc := range parsedDocs {
		row := make([]string, len(headers))
		for i, header := range headers {
			if val, ok := doc[header]; ok {
				var valStr string
				switch v := val.(type) {
				case map[string]any, []any:
					jsonVal, _ := stdjson.Marshal(v)
					valStr = string(jsonVal)
				case nil:
					valStr = "(nil)"
				default:
					valStr = fmt.Sprintf("%v", v)
				}
				row[i] = valStr
			} else {
				row[i] = "(n/a)"
			}
		}
		table.Append(row)
	}
	table.Render()
	return nil
}

func (c *cli) readRawResponse() (protocol.ResponseStatus, string, []byte, error) {

	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, statusByte); err != nil {
		return 0, "", nil, fmt.Errorf("failed to read response status from server: %w", err)
	}
	status := protocol.ResponseStatus(statusByte[0])

	msg, err := protocol.ReadString(c.conn)
	if err != nil {
		return status, "", nil, fmt.Errorf("failed to read response message from server: %w", err)
	}

	dataBytes, err := protocol.ReadBytes(c.conn)
	if err != nil {
		return status, msg, nil, fmt.Errorf("failed to read response data from server: %w", err)
	}

	return status, msg, dataBytes, nil
}
