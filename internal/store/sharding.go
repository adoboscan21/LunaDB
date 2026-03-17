package store

import "hash/fnv"

// GetShardID calcula la partición correspondiente para una llave dada.
// Utiliza FNV-1a, que es un algoritmo de hash muy rápido y con buena distribución.
func GetShardID(key string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	// Convertimos a int y aplicamos módulo para que caiga en el rango [0, numShards-1]
	return int(h.Sum32()) % numShards
}
