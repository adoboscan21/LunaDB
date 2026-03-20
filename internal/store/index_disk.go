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

// escapeString reemplaza 0x00 por 0x01 0x01, y 0x01 por 0x01 0x02.
// Garantiza que 0x00 NUNCA aparezca en el string, preservando el orden lexicográfico.
func escapeString(b []byte) []byte {
	var res []byte
	for _, ch := range b {
		switch ch {
		case 0x00:
			res = append(res, 0x01, 0x01)
		case 0x01:
			res = append(res, 0x01, 0x02)
		default:
			res = append(res, ch)
		}
	}
	return res
}

// unescapeString revierte el proceso de escape al leer de disco.
func unescapeString(b []byte) []byte {
	var res []byte
	for i := 0; i < len(b); i++ {
		if b[i] == 0x01 && i+1 < len(b) {
			switch b[i+1] {
			case 0x01:
				res = append(res, 0x00)
			case 0x02:
				res = append(res, 0x01)
			default:
				res = append(res, b[i]) // Corrupted fallback
			}
			i++
		} else {
			res = append(res, b[i])
		}
	}
	return res
}

// encodeIndexPrefix genera el prefijo de búsqueda para un valor añadiendo su tipo.
func encodeIndexPrefix(value any) []byte {
	if fVal, ok := valueToFloat64(value); ok {
		valBytes := make([]byte, 9)
		valBytes[0] = idxTypeFloat64 // Byte de Tipo
		bits := math.Float64bits(fVal)
		if fVal >= 0 {
			bits ^= 0x8000000000000000
		} else {
			bits ^= 0xFFFFFFFFFFFFFFFF
		}
		binary.BigEndian.PutUint64(valBytes[1:], bits)
		return append(valBytes, 0x00) // Terminador fijo
	} else if sVal, ok := value.(string); ok {
		escaped := escapeString([]byte(sVal))
		valBytes := make([]byte, 1, 1+len(escaped)+1)
		valBytes[0] = idxTypeString // Byte de Tipo
		valBytes = append(valBytes, escaped...)
		return append(valBytes, 0x00) // Terminador absoluto
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
		return string(unescapeString(data)), true
	}
	return nil, false
}

// extractValueFromIndexKey separa la porción del valor de una llave completa de bbolt.
func extractValueFromIndexKey(indexKey []byte) []byte {
	if len(indexKey) == 0 {
		return indexKey
	}

	switch indexKey[0] {
	case idxTypeFloat64:
		// Float64 es fijo: 1 tipo + 8 float + 1 term = 10 bytes
		if len(indexKey) >= 10 {
			return indexKey[:10]
		}
	case idxTypeString:
		// Como escapamos el string, el PRIMER 0x00 es 100% seguro el separador
		idx := bytes.IndexByte(indexKey[1:], 0x00)
		if idx != -1 {
			return indexKey[:1+idx+1] // Devolvemos hasta el terminador incluido
		}
	}

	return indexKey // Fallback
}

// encodeIndexKey genera la llave final para guardar en disco: [Tipo][Valor Escapado]\x00[DocID]
func encodeIndexKey(value any, docID string) []byte {
	prefix := encodeIndexPrefix(value)
	if prefix == nil {
		return nil
	}
	return append(prefix, []byte(docID)...)
}

// decodeDocID extrae el ID del documento desde una llave recuperada del disco en O(1) escaneo de byte.
func decodeDocID(indexKey []byte) string {
	if len(indexKey) == 0 {
		return ""
	}

	switch indexKey[0] {
	case idxTypeFloat64:
		if len(indexKey) > 10 {
			return string(indexKey[10:]) // Todo lo que esté después del byte 10 es el DocID
		}
	case idxTypeString:
		// Usamos IndexByte en lugar de LastIndexByte (buscamos de izq a der)
		idx := bytes.IndexByte(indexKey[1:], 0x00)
		if idx != -1 && 1+idx+1 < len(indexKey) {
			return string(indexKey[1+idx+1:])
		}
	}

	return ""
}
