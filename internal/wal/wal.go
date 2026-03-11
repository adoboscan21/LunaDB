/* ==========================================================
   Ruta y Archivo: ./internal/wal/wal.go
   ========================================================== */

package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"lunadb/internal/protocol"
	"os"
	"time"
)

// WalEntry represents a single operation recorded in the log.
type WalEntry struct {
	CommandType protocol.CommandType
	Payload     []byte
}

type walRequest struct {
	entry    *WalEntry
	rotateCh chan error
	errCh    chan error
}

// WAL (Write-Ahead Log) manages the writing and reading of the durability log.
// OPTIMIZADO: Utiliza Smart Group Commit (Drain-and-Flush) sin latencia artificial.
type WAL struct {
	reqChan chan walRequest
	path    string
}

// New creates and initializes a new WAL instance at the specified path.
func New(path string, syncInterval time.Duration) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	w := &WAL{
		reqChan: make(chan walRequest, 10000),
		path:    path,
	}

	go w.worker(file, syncInterval) // <-- Le pasamos el intervalo
	return w, nil
}

// worker es el demonio de Group Commit. Agrupa escrituras concurrentes inteligentemente.
func (w *WAL) worker(initialFile *os.File, syncInterval time.Duration) {
	file := initialFile
	writer := bufio.NewWriterSize(file, 1024*1024) // Buffer de 1MB
	var pendingReqs []walRequest

	var ticker *time.Ticker
	var tickerCh <-chan time.Time
	if syncInterval > 0 {
		ticker = time.NewTicker(syncInterval)
		tickerCh = ticker.C
	}

	// Función helper para vaciar la memoria al OS y responder a los clientes de inmediato.
	flushAndNotify := func() {
		if len(pendingReqs) == 0 {
			return
		}

		// 1. Escribimos al OS de forma instantánea
		err := writer.Flush()

		// 2. Si estamos en modo paranoico (0s), obligamos al disco físico a girar en cada petición
		if err == nil && syncInterval == 0 {
			err = file.Sync()
		}

		// 3. Liberamos a los clientes!
		for _, req := range pendingReqs {
			if req.errCh != nil {
				req.errCh <- err
			}
		}
		pendingReqs = pendingReqs[:0]
	}

	for {
		select {
		case req, ok := <-w.reqChan:
			if !ok { // Cierre solicitado
				flushAndNotify()
				if syncInterval > 0 {
					file.Sync()
				}
				file.Close()
				return
			}

			if req.rotateCh != nil { // Rotación solicitada
				flushAndNotify()
				if syncInterval > 0 {
					file.Sync()
				}
				file.Close()
				os.Remove(w.path)
				file, _ = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				writer.Reset(file)
				req.rotateCh <- nil
				continue
			}

			// Procesar petición
			totalLen := 1 + len(req.entry.Payload)
			binary.Write(writer, binary.LittleEndian, uint32(totalLen))
			writer.WriteByte(byte(req.entry.CommandType))
			writer.Write(req.entry.Payload)
			pendingReqs = append(pendingReqs, req)

			// DRAIN: Agrupar peticiones que hayan llegado simultáneamente
			draining := true
			for draining && len(pendingReqs) < 5000 {
				select {
				case nextReq, nextOk := <-w.reqChan:
					if !nextOk {
						draining = false
						break
					}
					if nextReq.rotateCh != nil {
						flushAndNotify()
						if syncInterval > 0 {
							file.Sync()
						}
						file.Close()
						os.Remove(w.path)
						file, _ = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						writer.Reset(file)
						nextReq.rotateCh <- nil
					} else {
						totalLen := 1 + len(nextReq.entry.Payload)
						binary.Write(writer, binary.LittleEndian, uint32(totalLen))
						writer.WriteByte(byte(nextReq.entry.CommandType))
						writer.Write(nextReq.entry.Payload)
						pendingReqs = append(pendingReqs, nextReq)
					}
				default:
					draining = false
				}
			}

			// Escribir en OS y notificar a todos los clientes del batch
			flushAndNotify()

		// Sincronización en segundo plano con el disco duro físico
		case <-tickerCh:
			if writer.Buffered() > 0 {
				writer.Flush()
			}
			file.Sync()
		}
	}
}

// Write escribe de forma segura en el WAL. Bloquea hasta que la escritura está en disco.
func (w *WAL) Write(entry WalEntry) error {
	errCh := make(chan error, 1)
	w.reqChan <- walRequest{entry: &entry, errCh: errCh}
	return <-errCh // Esperar al Group Commit
}

// Close closes the WAL file safely.
func (w *WAL) Close() error {
	close(w.reqChan)
	return nil
}

// Rotate closes the current WAL file, deletes it, and opens a new one in its place.
func (w *WAL) Rotate() error {
	rotateCh := make(chan error, 1)
	w.reqChan <- walRequest{rotateCh: rotateCh}
	return <-rotateCh // Esperar a que el worker termine la rotación
}

// Path returns the file path of the WAL.
func (w *WAL) Path() string {
	return w.path
}

// Replay reads all entries from the WAL file and sends them to a channel.
func Replay(path string) (<-chan WalEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("WAL file not found, skipping replay.", "path", path)
			closeChan := make(chan WalEntry)
			close(closeChan)
			return closeChan, nil
		}
		return nil, fmt.Errorf("failed to open WAL file for replay: %w", err)
	}

	entriesChan := make(chan WalEntry, 100)

	go func() {
		defer file.Close()
		defer close(entriesChan)

		reader := bufio.NewReader(file)
		for {
			var totalLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &totalLen); err != nil {
				if err != io.EOF {
					slog.Error("Failed to read WAL entry length during replay", "error", err)
				}
				break
			}

			entryData := make([]byte, totalLen)
			if _, err := io.ReadFull(reader, entryData); err != nil {
				slog.Error("Failed to read full WAL entry during replay", "error", err)
				break
			}

			entry := WalEntry{
				CommandType: protocol.CommandType(entryData[0]),
				Payload:     entryData[1:],
			}
			entriesChan <- entry
		}
		slog.Info("WAL replay finished.", "path", path)
	}()

	return entriesChan, nil
}
