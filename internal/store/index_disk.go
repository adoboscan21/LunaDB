package store

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	idxTypeFloat64 byte = 1
	idxTypeString  byte = 2
)

// encodeIndexPrefix genera el prefijo de búsqueda para un valor añadiendo su tipo.
func encodeIndexPrefix(value any) []byte {
	if fVal, ok := valueToFloat64(value); ok {
		valBytes := make([]byte, 9)
		valBytes[0] = idxTypeFloat64 // <--- Byte de Tipo
		bits := math.Float64bits(fVal)
		if fVal >= 0 {
			bits ^= 0x8000000000000000
		} else {
			bits ^= 0xFFFFFFFFFFFFFFFF
		}
		binary.BigEndian.PutUint64(valBytes[1:], bits)
		return append(valBytes, 0x00)
	} else if sVal, ok := value.(string); ok {
		valBytes := make([]byte, 1+len(sVal))
		valBytes[0] = idxTypeString // <--- Byte de Tipo
		copy(valBytes[1:], sVal)
		return append(valBytes, 0x00)
	}
	return nil
}

// decodeIndexValue hace el proceso inverso: extrae el valor original desde los bytes del disco.
func decodeIndexValue(raw []byte) (any, bool) {
	if len(raw) < 1 {
		return nil, false
	}
	typ := raw[0]
	data := raw[1:]

	// Quitamos el byte nulo del final si lo tiene
	if len(data) > 0 && data[len(data)-1] == 0x00 {
		data = data[:len(data)-1]
	}

	if typ == idxTypeFloat64 && len(data) == 8 {
		bits := binary.BigEndian.Uint64(data)
		if (bits & 0x8000000000000000) != 0 {
			bits ^= 0x8000000000000000
		} else {
			bits ^= 0xFFFFFFFFFFFFFFFF
		}
		return math.Float64frombits(bits), true
	} else if typ == idxTypeString {
		return string(data), true
	}
	return nil, false
}

// extractValueFromIndexKey separa la porción del valor de una llave completa de bbolt
func extractValueFromIndexKey(indexKey []byte) []byte {
	idx := bytes.LastIndexByte(indexKey, 0x00)
	if idx == -1 {
		return indexKey
	}
	return indexKey[:idx+1]
}

// encodeIndexKey genera la llave final para guardar en disco: [Tipo][Valor]\x00[DocID]
func encodeIndexKey(value any, docID string) []byte {
	prefix := encodeIndexPrefix(value)
	if prefix == nil {
		return nil
	}
	return append(prefix, []byte(docID)...)
}

// decodeDocID extrae el ID del documento desde una llave recuperada del disco.
func decodeDocID(indexKey []byte) string {
	idx := bytes.LastIndexByte(indexKey, 0x00)
	if idx == -1 {
		return ""
	}
	return string(indexKey[idx+1:])
}
