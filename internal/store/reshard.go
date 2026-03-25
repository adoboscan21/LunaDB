package store

import (
	"fmt"
	"strings"
	"sync"

	"go.etcd.io/bbolt"
)

// UnifiedBackup consolida todos los shards físicos en un único archivo de backup monolítico.
func UnifiedBackup(destPath string) error {
	backupDB, err := bbolt.Open(destPath, 0600, &bbolt.Options{NoFreelistSync: true})
	if err != nil {
		return fmt.Errorf("could not create backup db: %w", err)
	}
	defer backupDB.Close()

	for i := 0; i < TotalShards; i++ {
		err = GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				return backupDB.Update(func(btx *bbolt.Tx) error {
					destB, err := btx.CreateBucketIfNotExists(name)
					if err != nil {
						return err
					}
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						if err := destB.Put(k, v); err != nil {
							return err
						}
					}
					return nil
				})
			})
		})
		if err != nil {
			return fmt.Errorf("error backing up shard %d: %w", i, err)
		}
	}
	return nil
}

// UnifiedRestore lee un backup monolítico y re-distribuye (Re-Shard) los datos
// a la cantidad actual de particiones (TotalShards).
func UnifiedRestore(backupPath string) error {
	backupDB, err := bbolt.Open(backupPath, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("could not open backup db: %w", err)
	}
	defer backupDB.Close()

	// 1. Limpieza de los shards actuales
	for i := 0; i < TotalShards; i++ {
		err = GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			var bucketsToDelete [][]byte
			tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				bucketsToDelete = append(bucketsToDelete, name)
				return nil
			})
			for _, name := range bucketsToDelete {
				tx.DeleteBucket(name)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to wipe shard %d: %w", i, err)
		}
	}

	// 2. Re-Sharding y carga paralela
	return backupDB.View(func(btx *bbolt.Tx) error {
		return btx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			bucketName := string(name)
			isIndex := strings.HasPrefix(bucketName, "_idx_")

			type kv struct{ k, v []byte }
			shardChunks := make([][]kv, TotalShards)
			count := 0
			chunkSize := 20000 // Inserción en lotes para no saturar la RAM

			flush := func() error {
				var wg sync.WaitGroup
				errs := make(chan error, TotalShards)

				for i := 0; i < TotalShards; i++ {
					if len(shardChunks[i]) == 0 {
						continue
					}
					wg.Add(1)
					go func(shardID int, items []kv) {
						defer wg.Done()
						err := GlobalDBs[shardID].Update(func(gtx *bbolt.Tx) error {
							destB, err := gtx.CreateBucketIfNotExists(name)
							if err != nil {
								return err
							}
							for _, item := range items {
								if err := destB.Put(item.k, item.v); err != nil {
									return err
								}
							}
							return nil
						})
						if err != nil {
							errs <- err
						}
					}(i, shardChunks[i])
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						return err
					}
				}
				shardChunks = make([][]kv, TotalShards)
				return nil
			}

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var docID string
				if isIndex {
					// Extraemos dinámicamente el DocID desde la llave binaria del índice
					docID = decodeDocID(k)
				} else {
					docID = string(k)
				}

				// Magia: Se recalcula el Shard en base a la configuración actual de hardware
				shardID := GetShardID(docID, TotalShards)

				kCopy := make([]byte, len(k))
				copy(kCopy, k)
				vCopy := make([]byte, len(v))
				copy(vCopy, v)

				shardChunks[shardID] = append(shardChunks[shardID], kv{kCopy, vCopy})
				count++

				if count >= chunkSize {
					if err := flush(); err != nil {
						return err
					}
					count = 0
				}
			}
			return flush()
		})
	})
}
