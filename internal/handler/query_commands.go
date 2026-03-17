package handler

import (
	"container/heap"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"

	"lunadb/internal/globalconst"
	"lunadb/internal/protocol"
	"lunadb/internal/store"
)

var regexCache sync.Map

// handleCollectionQuery procesa el comando CmdCollectionQuery.
func (h *ConnectionHandler) handleCollectionQuery(r io.Reader, conn net.Conn) {
	collectionName, queryBSONBytes, err := protocol.ReadCollectionQueryCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_QUERY command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_QUERY command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		slog.Warn("Unauthorized query attempt",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for query", collectionName), nil)
		return
	}

	query := queryPool.Get().(*Query)
	defer func() {
		query.Reset()
		queryPool.Put(query)
	}()

	if err := bson.Unmarshal(queryBSONBytes, query); err != nil {
		slog.Warn("Failed to unmarshal query BSON",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query BSON format", nil)
		return
	}

	slog.Debug("Processing collection query", "user", h.AuthenticatedUser, "collection", collectionName)

	results, err := h.processCollectionQuery(collectionName, query)
	if err != nil {
		slog.Error("Error processing collection query",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to execute query: %v", err), nil)
		return
	}

	responseBytes, err := bson.Marshal(bson.M{"results": results})
	if err != nil {
		slog.Error("Error marshalling query results",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal query results", nil)
		return
	}

	if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Query executed on collection '%s'", collectionName), responseBytes); err != nil {
		slog.Error("Failed to write COLLECTION_QUERY response", "error", err, "remote_addr", conn.RemoteAddr().String())
	}
}

func (h *ConnectionHandler) processCollectionQuery(collectionName string, query *Query) (any, error) {
	colStore := h.CollectionManager.GetCollection(collectionName)

	// =====================================================================
	// 🚀 1. OPTIMIZACIÓN LETAL: INDEX-ONLY SCAN (CORTOCIRCUITO ABSOLUTO)
	// =====================================================================

	if query.Distinct != "" && len(query.Filter) == 0 {
		if distinctVals, ok := colStore.GetDistinctValues(query.Distinct); ok {
			return distinctVals, nil
		}
	}

	if len(query.GroupBy) == 1 && len(query.Filter) == 0 && len(query.Having) == 0 {
		isOnlyCount := true
		for _, agg := range query.Aggregations {
			if agg.Func != globalconst.AggCount || agg.Field != "*" {
				isOnlyCount = false
				break
			}
		}
		if isOnlyCount && len(query.Aggregations) > 0 {
			if groupedCounts, ok := colStore.GetGroupedCount(query.GroupBy[0]); ok {
				var results []bson.M
				for val, count := range groupedCounts {
					row := bson.M{query.GroupBy[0]: val}
					for aggName := range query.Aggregations {
						row[aggName] = count
					}
					results = append(results, row)
				}
				return results, nil
			}
		}
	}

	// =====================================================================
	// 🚀 1.5. OPTIMIZACIÓN: STREAMING AGGREGATION (ON-THE-FLY)
	// =====================================================================

	isPureAggregation := len(query.Filter) == 0 && len(query.Lookups) == 0 && len(query.Projection) == 0 && len(query.Aggregations) > 0 && query.Distinct == ""

	if isPureAggregation {
		return h.streamAggregations(colStore, query)
	}

	// =====================================================================
	// 2. EJECUCIÓN NORMAL (Carga de datos transaccional desde disco/bbolt)
	// =====================================================================

	isSimpleQuery := len(query.Filter) == 0 && len(query.OrderBy) == 0 &&
		len(query.Aggregations) == 0 && len(query.GroupBy) == 0 &&
		query.Distinct == "" && len(query.Lookups) == 0 && len(query.Projection) == 0 && !query.Count

	if isSimpleQuery {
		capacity := 1024
		if query.Limit != nil && *query.Limit > 0 {
			capacity = *query.Limit
		}
		rawResults := make([]bson.Raw, 0, capacity)
		processedCount, limit := 0, -1
		if query.Limit != nil {
			limit = *query.Limit
		}

		colStore.StreamAll(func(key string, value []byte) bool {
			if processedCount < query.Offset {
				processedCount++
				return true
			}
			rawResults = append(rawResults, bson.Raw(value))
			if limit != -1 && len(rawResults) >= limit {
				return false
			}
			return true
		})
		return rawResults, nil
	}

	var paginatedRaw []bson.Raw
	usedFastPath := false

	canShortCircuit := query.Limit != nil && len(query.OrderBy) == 0 && len(query.Aggregations) == 0 && len(query.GroupBy) == 0
	var limitTarget int
	if canShortCircuit {
		limitTarget = *query.Limit + query.Offset
	}

	// --- 🚀 OPTIMIZACIÓN DE PAGINACIÓN PROFUNDA: INDEX FAST PATH (ORDER BY + LIMIT) ---
	if len(query.OrderBy) == 1 && len(query.Aggregations) == 0 && len(query.GroupBy) == 0 && query.Distinct == "" && !query.Count {
		orderField := query.OrderBy[0].Field
		isDesc := query.OrderBy[0].Direction == globalconst.SortDesc

		if colStore.HasIndex(orderField) {
			limit := -1
			if query.Limit != nil {
				limit = *query.Limit
			}
			offset := query.Offset
			matchCount := 0
			seenKeys := make(map[string]struct{})

			if len(query.Filter) == 0 {
				colStore.StreamByIndex(orderField, isDesc, func(key string) bool {
					if _, seen := seenKeys[key]; seen {
						return true
					}
					seenKeys[key] = struct{}{}

					matchCount++
					if matchCount > offset {
						if vBytes, found := colStore.Get(key); found {
							paginatedRaw = append(paginatedRaw, bson.Raw(vBytes))
							if limit != -1 && len(paginatedRaw) >= limit {
								return false // Cortocircuito absoluto
							}
						}
					}
					return true
				})
				if limit != -1 && len(paginatedRaw) == limit {
					usedFastPath = true
					slog.Debug("Query optimizer: used B-Tree Fast Path for DEEP PAGINATION")
				} else if limit == -1 {
					usedFastPath = true
				}
			} else {
				compiledEvaluator := h.compileFilter(query.Filter)
				colStore.StreamByIndex(orderField, isDesc, func(key string) bool {
					if _, seen := seenKeys[key]; seen {
						return true
					}
					seenKeys[key] = struct{}{}

					if vBytes, found := colStore.Get(key); found {
						rawDoc := bson.Raw(vBytes)
						if compiledEvaluator(rawDoc) {
							matchCount++
							if matchCount > offset {
								paginatedRaw = append(paginatedRaw, rawDoc)
								if limit != -1 && len(paginatedRaw) >= limit {
									return false
								}
							}
						}
					}
					return true
				})
				if limit != -1 && len(paginatedRaw) == limit {
					usedFastPath = true
					slog.Debug("Query optimizer: used B-Tree Fast Path for ORDER BY + LIMIT + FILTER")
				} else {
					paginatedRaw = nil
				}
			}
		}
	}

	var finalRawResults []bson.Raw

	if !usedFastPath {
		var candidateKeys []string
		var usedIndex bool
		var remainingFilter map[string]any

		_, hasOr := query.Filter[globalconst.OpOr]

		if canShortCircuit && hasOr {
			usedIndex = false
			remainingFilter = query.Filter
		} else {
			candidateKeys, usedIndex, remainingFilter = h.findCandidateKeysFromFilter(colStore, query.Filter)
		}

		compiledEvaluator := h.compileFilter(remainingFilter)

		if usedIndex {
			uniqueKeys := make(map[string]struct{}, len(candidateKeys))
			dedupedKeys := make([]string, 0, len(candidateKeys))
			for _, k := range candidateKeys {
				if _, exists := uniqueKeys[k]; !exists {
					uniqueKeys[k] = struct{}{}
					dedupedKeys = append(dedupedKeys, k)
				}
			}
			candidateKeys = dedupedKeys

			numKeys := len(candidateKeys)

			if numKeys > 1000 && !canShortCircuit {
				numWorkers := 4
				chunkSize := (numKeys + numWorkers - 1) / numWorkers
				var wg sync.WaitGroup
				var mu sync.Mutex

				finalRawResults = make([]bson.Raw, 0, numKeys/2)

				for i := 0; i < numWorkers; i++ {
					start := i * chunkSize
					end := start + chunkSize
					if end > numKeys {
						end = numKeys
					}
					if start >= numKeys {
						break
					}

					wg.Add(1)
					go func(keysChunk []string) {
						defer wg.Done()
						localResults := make([]bson.Raw, 0, len(keysChunk)/2)

						for _, k := range keysChunk {
							if vBytes, found := colStore.Get(k); found {
								rawDoc := bson.Raw(vBytes)
								if compiledEvaluator(rawDoc) {
									localResults = append(localResults, rawDoc)
								}
							}
						}

						if len(localResults) > 0 {
							mu.Lock()
							finalRawResults = append(finalRawResults, localResults...)
							mu.Unlock()
						}
					}(candidateKeys[start:end])
				}
				wg.Wait()
			} else {
				capacity := 1024
				if canShortCircuit {
					capacity = limitTarget
				}
				finalRawResults = make([]bson.Raw, 0, capacity)

				for _, k := range candidateKeys {
					if vBytes, found := colStore.Get(k); found {
						rawDoc := bson.Raw(vBytes)
						if compiledEvaluator(rawDoc) {
							finalRawResults = append(finalRawResults, rawDoc)

							if canShortCircuit && len(finalRawResults) >= limitTarget {
								break
							}
						}
					}
				}
			}
		} else {
			capacity := 1024
			if canShortCircuit {
				capacity = limitTarget
			}
			finalRawResults = make([]bson.Raw, 0, capacity)

			colStore.StreamAll(func(k string, vBytes []byte) bool {
				rawDoc := bson.Raw(vBytes)
				if compiledEvaluator(rawDoc) {
					finalRawResults = append(finalRawResults, rawDoc)

					if canShortCircuit && len(finalRawResults) >= limitTarget {
						return false
					}
				}
				return true
			})
		}

		if query.Distinct != "" {
			distinctValues := make(map[any]bool)
			var resultList []any
			for _, raw := range finalRawResults {
				if val, ok := getRawValue(raw, query.Distinct); ok && val != nil {
					if _, seen := distinctValues[val]; !seen {
						distinctValues[val] = true
						resultList = append(resultList, val)
					}
				}
			}
			return resultList, nil
		}

		if query.Count && len(query.Aggregations) == 0 && len(query.GroupBy) == 0 {
			return bson.M{globalconst.AggCount: len(finalRawResults)}, nil
		}

		if len(query.Aggregations) > 0 || len(query.GroupBy) > 0 {
			return h.performRawAggregations(finalRawResults, query)
		}

		if len(query.OrderBy) > 0 {
			sortItems := make([]SortItem, 0, len(finalRawResults))
			for _, raw := range finalRawResults {
				vals := make([]any, len(query.OrderBy))
				for i, ob := range query.OrderBy {
					if val, ok := getRawValue(raw, ob.Field); ok {
						vals[i] = val
					}
				}
				sortItems = append(sortItems, SortItem{Raw: raw, Values: vals})
			}

			if query.Limit != nil && *query.Limit > 0 {
				limit := *query.Limit
				needed := query.Offset + limit

				if needed >= len(sortItems) {
					sortRawResults(sortItems, query.OrderBy)
					paginatedRaw = make([]bson.Raw, len(sortItems))
					for i, item := range sortItems {
						paginatedRaw[i] = item.Raw
					}
				} else {
					pq := &RawResultHeap{items: make([]SortItem, 0, needed), orderBy: query.OrderBy}
					heap.Init(pq)
					for _, item := range sortItems {
						if pq.Len() < needed {
							heap.Push(pq, item)
						} else {
							if pq.LessItem(item, pq.items[0]) {
								heap.Pop(pq)
								heap.Push(pq, item)
							}
						}
					}
					paginatedRaw = make([]bson.Raw, pq.Len())
					for i := pq.Len() - 1; i >= 0; i-- {
						paginatedRaw[i] = heap.Pop(pq).(SortItem).Raw
					}
				}
			} else {
				sortRawResults(sortItems, query.OrderBy)
				paginatedRaw = make([]bson.Raw, len(sortItems))
				for i, item := range sortItems {
					paginatedRaw[i] = item.Raw
				}
			}
		} else {
			paginatedRaw = finalRawResults
		}

		offset := min(max(query.Offset, 0), len(paginatedRaw))
		paginatedRaw = paginatedRaw[offset:]
		if query.Limit != nil && *query.Limit >= 0 {
			if *query.Limit < len(paginatedRaw) {
				paginatedRaw = paginatedRaw[:*query.Limit]
			}
		}
	}

	// --- LATE UNMARSHAL ---
	finalDocs := make([]bson.M, 0, len(paginatedRaw))
	for _, raw := range paginatedRaw {
		var doc bson.M
		if err := bson.Unmarshal(raw, &doc); err == nil {
			finalDocs = append(finalDocs, doc)
		}
	}

	// --- ZERO-COPY CACHED JOIN ---
	if len(query.Lookups) > 0 {
		for _, lookupSpec := range query.Lookups {
			foreignColStore := h.CollectionManager.GetCollection(lookupSpec.FromCollection)
			hasIndex := foreignColStore != nil && foreignColStore.HasIndex(lookupSpec.ForeignField)

			if !hasIndex {
				for i := range finalDocs {
					finalDocs[i][lookupSpec.As] = nil
				}
				continue
			}

			localCache := make(map[any]any)
			isFlatKey := !strings.Contains(lookupSpec.LocalField, ".")

			for i := range finalDocs {
				var localVal any
				var ok bool

				if isFlatKey {
					localVal, ok = finalDocs[i][lookupSpec.LocalField]
				} else {
					localVal, ok = getNestedValue(finalDocs[i], lookupSpec.LocalField)
				}

				if ok && localVal != nil {
					var cacheKey any
					switch v := localVal.(type) {
					case string, int, int32, int64, float64, float32, bool:
						cacheKey = v
					default:
						cacheKey = fmt.Sprintf("%v", v)
					}

					if cachedDoc, exists := localCache[cacheKey]; exists {
						finalDocs[i][lookupSpec.As] = cachedDoc
						continue
					}

					if keys, found := foreignColStore.Lookup(lookupSpec.ForeignField, localVal); found && len(keys) > 0 {
						if fBytes, existsInForeign := foreignColStore.Get(keys[0]); existsInForeign {
							rawForeign := bson.Raw(fBytes)
							finalDocs[i][lookupSpec.As] = rawForeign
							localCache[cacheKey] = rawForeign
							continue
						}
					}
					localCache[cacheKey] = nil
				}
				finalDocs[i][lookupSpec.As] = nil
			}
		}
	}

	// Projection
	if len(query.Projection) > 0 {
		projectedResults := make([]bson.M, 0, len(finalDocs))
		for _, fullDoc := range finalDocs {
			projectedDoc := make(bson.M)
			for _, fieldPath := range query.Projection {
				if value, ok := getNestedValue(fullDoc, fieldPath); ok {
					setNestedValue(projectedDoc, fieldPath, value)
				}
			}
			projectedResults = append(projectedResults, projectedDoc)
		}
		return projectedResults, nil
	}

	return finalDocs, nil
}

// sortResults ordena un slice completo usando sort.Slice.
func (h *ConnectionHandler) sortResults(results []bson.M, orderBy []OrderByClause) {
	sort.Slice(results, func(i, j int) bool {
		for _, ob := range orderBy {
			valA, okA := getNestedValue(results[i], ob.Field)
			valB, okB := getNestedValue(results[j], ob.Field)

			if !okA && !okB {
				continue
			}
			if !okA {
				return true
			}
			if !okB {
				return false
			}

			cmp := compare(valA, valB)
			if cmp != 0 {
				if ob.Direction == globalconst.SortDesc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// --- IMPLEMENTACIÓN DE HEAP PARA TOP-K SORT ---

type ResultHeap struct {
	items   []bson.M
	orderBy []OrderByClause
}

func (h *ResultHeap) Len() int      { return len(h.items) }
func (h *ResultHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *ResultHeap) Push(x any)    { h.items = append(h.items, x.(bson.M)) }
func (h *ResultHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}
func (h *ResultHeap) Less(i, j int) bool { return !h.LessItem(h.items[i], h.items[j]) }

func (h *ResultHeap) LessItem(a, b bson.M) bool {
	for _, ob := range h.orderBy {
		valA, okA := getNestedValue(a, ob.Field)
		valB, okB := getNestedValue(b, ob.Field)

		if !okA && !okB {
			continue
		}
		if !okA {
			return true
		}
		if !okB {
			return false
		}

		cmp := compare(valA, valB)
		if cmp != 0 {
			if ob.Direction == globalconst.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return false
}

// --- OPTIMIZADOR DE QUERIES (Index Selection) ---

func (h *ConnectionHandler) findCandidateKeysFromFilter(colStore store.DataStore, filter map[string]any) (keys []string, usedIndex bool, remainingFilter map[string]any) {
	if len(filter) == 0 {
		return nil, false, filter
	}

	if orRaw, exists := filter[globalconst.OpOr]; exists {
		var orConditions []any
		if ba, ok := orRaw.(bson.A); ok {
			orConditions = ba
		} else if arr, ok := orRaw.([]any); ok {
			orConditions = arr
		}

		if len(orConditions) > 0 {
			unionKeys := make(map[string]struct{})
			allConditionsAreIndexable := true
			hasAnyRemainingFilter := false

			for _, cond := range orConditions {
				condMap, isMap := asMap(cond)
				if !isMap {
					allConditionsAreIndexable = false
					break
				}
				subKeys, subIndexUsed, subRemainingFilter := h.findCandidateKeysFromFilter(colStore, condMap)
				if !subIndexUsed {
					allConditionsAreIndexable = false
					break
				}
				if len(subRemainingFilter) > 0 {
					hasAnyRemainingFilter = true
				}
				for _, key := range subKeys {
					unionKeys[key] = struct{}{}
				}
			}

			if allConditionsAreIndexable {
				finalKeys := make([]string, 0, len(unionKeys))
				for k := range unionKeys {
					finalKeys = append(finalKeys, k)
				}
				slog.Debug("Query optimizer: using Index Merge for 'OR' clause", "found_keys", len(finalKeys))

				var finalRemainingFilter map[string]any
				if hasAnyRemainingFilter {
					finalRemainingFilter = filter
				} else {
					finalRemainingFilter = make(map[string]any)
				}
				return finalKeys, true, finalRemainingFilter
			}
		}
	}

	if andRaw, exists := filter[globalconst.OpAnd]; exists {
		var andConditions []any
		if ba, ok := andRaw.(bson.A); ok {
			andConditions = ba
		} else if arr, ok := andRaw.([]any); ok {
			andConditions = arr
		}

		if len(andConditions) > 0 {
			keySets := [][]string{}
			nonIndexedConditions := bson.A{}

			for _, cond := range andConditions {
				condMap, isMap := asMap(cond)
				if !isMap {
					nonIndexedConditions = append(nonIndexedConditions, cond)
					continue
				}

				subKeys, subIndexUsed, subRemainingFilter := h.findCandidateKeysFromFilter(colStore, condMap)

				if subIndexUsed {
					keySets = append(keySets, subKeys)
					if len(subRemainingFilter) > 0 {
						nonIndexedConditions = append(nonIndexedConditions, subRemainingFilter)
					}
				} else {
					nonIndexedConditions = append(nonIndexedConditions, condMap)
				}
			}

			if len(keySets) > 0 {
				sort.Slice(keySets, func(i, j int) bool {
					return len(keySets[i]) < len(keySets[j])
				})

				intersectionMap := make(map[string]struct{}, len(keySets[0]))
				for _, key := range keySets[0] {
					intersectionMap[key] = struct{}{}
				}

				for i := 1; i < len(keySets); i++ {
					if len(intersectionMap) == 0 {
						break
					}
					currentSetMap := make(map[string]struct{})
					for _, key := range keySets[i] {
						if _, found := intersectionMap[key]; found {
							currentSetMap[key] = struct{}{}
						}
					}
					intersectionMap = currentSetMap
				}

				finalKeys := make([]string, 0, len(intersectionMap))
				for key := range intersectionMap {
					finalKeys = append(finalKeys, key)
				}

				newFilter := make(map[string]any)
				if len(nonIndexedConditions) > 0 {
					newFilter[globalconst.OpAnd] = nonIndexedConditions
				}
				return finalKeys, true, newFilter
			}
		}
	}

	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if fieldOk && opOk && colStore.HasIndex(field) {
		var keys []string
		var used bool

		switch op {
		case globalconst.OpEqual:
			keys, used = colStore.Lookup(field, value)
		case globalconst.OpIn:
			if values, isSlice := value.(bson.A); isSlice {
				unionKeys := make(map[string]struct{})
				for _, v := range values {
					lookupKeys, _ := colStore.Lookup(field, v)
					for _, key := range lookupKeys {
						unionKeys[key] = struct{}{}
					}
				}
				finalKeys := make([]string, 0, len(unionKeys))
				for k := range unionKeys {
					finalKeys = append(finalKeys, k)
				}
				keys = finalKeys
				used = true
			}
		case globalconst.OpGreaterThan:
			keys, used = colStore.LookupRange(field, value, nil, false, false)
		case globalconst.OpGreaterThanOrEqual:
			keys, used = colStore.LookupRange(field, value, nil, true, false)
		case globalconst.OpLessThan:
			keys, used = colStore.LookupRange(field, nil, value, false, false)
		case globalconst.OpLessThanOrEqual:
			keys, used = colStore.LookupRange(field, nil, value, false, true)
		case globalconst.OpBetween:
			if bounds, ok := value.(bson.A); ok && len(bounds) == 2 {
				keys, used = colStore.LookupRange(field, bounds[0], bounds[1], true, true)
			} else if bounds, ok := value.([]any); ok && len(bounds) == 2 {
				keys, used = colStore.LookupRange(field, bounds[0], bounds[1], true, true)
			}
		case globalconst.OpLike:
			if strVal, isStr := value.(string); isStr {
				if strings.HasSuffix(strVal, "%") && !strings.HasPrefix(strVal, "%") {
					prefix := strVal[:len(strVal)-1]
					if !strings.Contains(prefix, "%") {
						highBound := prefix + "\xff"
						keys, used = colStore.LookupRange(field, prefix, highBound, true, true)
					}
				}
			}
		}

		if used {
			slog.Debug("Query optimizer: using index for simple filter", "field", field, "op", op, "found_keys", len(keys))
			return keys, true, make(map[string]any)
		}
	}

	return nil, false, filter
}

func (h *ConnectionHandler) compileFilter(filter map[string]any) func(bson.Raw) bool {
	if len(filter) == 0 {
		return func(bson.Raw) bool { return true }
	}

	if andRaw, exists := filter[globalconst.OpAnd]; exists {
		var andConditions []any
		if ba, ok := andRaw.(bson.A); ok {
			andConditions = ba
		} else if arr, ok := andRaw.([]any); ok {
			andConditions = arr
		}
		var compiledConds []func(bson.Raw) bool
		for _, cond := range andConditions {
			if condMap, isM := asMap(cond); isM {
				compiledConds = append(compiledConds, h.compileFilter(condMap))
			}
		}
		return func(doc bson.Raw) bool {
			for _, fn := range compiledConds {
				if !fn(doc) {
					return false
				}
			}
			return true
		}
	}

	if orRaw, exists := filter[globalconst.OpOr]; exists {
		var orConditions []any
		if ba, ok := orRaw.(bson.A); ok {
			orConditions = ba
		} else if arr, ok := orRaw.([]any); ok {
			orConditions = arr
		}
		var compiledConds []func(bson.Raw) bool
		for _, cond := range orConditions {
			if condMap, isM := asMap(cond); isM {
				compiledConds = append(compiledConds, h.compileFilter(condMap))
			}
		}
		return func(doc bson.Raw) bool {
			for _, fn := range compiledConds {
				if fn(doc) {
					return true
				}
			}
			return false
		}
	}

	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if !fieldOk || !opOk {
		return func(bson.Raw) bool { return false }
	}

	keys := strings.Split(field, ".")

	switch op {
	case globalconst.OpEqual:
		return func(doc bson.Raw) bool {
			rawVal, err := doc.LookupErr(keys...)
			if err != nil || rawVal.Type == bsontype.Null {
				return value == nil
			}
			var itemVal any
			switch rawVal.Type {
			case bsontype.String:
				itemVal = rawVal.StringValue()
			case bsontype.Int32:
				itemVal = int64(rawVal.Int32())
			case bsontype.Int64:
				itemVal = rawVal.Int64()
			case bsontype.Double:
				itemVal = rawVal.Double()
			case bsontype.Boolean:
				itemVal = rawVal.Boolean()
			}
			return compare(itemVal, value) == 0
		}
	case globalconst.OpBetween:
		var b0, b1 any
		if ba, ok := value.(bson.A); ok && len(ba) == 2 {
			b0, b1 = ba[0], ba[1]
		} else if arr, ok := value.([]any); ok && len(arr) == 2 {
			b0, b1 = arr[0], arr[1]
		}
		return func(doc bson.Raw) bool {
			rawVal, err := doc.LookupErr(keys...)
			if err != nil || rawVal.Type == bsontype.Null {
				return false
			}
			var itemVal any
			switch rawVal.Type {
			case bsontype.Int32:
				itemVal = int64(rawVal.Int32())
			case bsontype.Int64:
				itemVal = rawVal.Int64()
			case bsontype.Double:
				itemVal = rawVal.Double()
			default:
				return false
			}
			return compare(itemVal, b0) >= 0 && compare(itemVal, b1) <= 0
		}
	case globalconst.OpLike:
		if pattern, isStrPattern := value.(string); isStrPattern {
			pattern = strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
			pattern = "(?i)^" + pattern + "$"
			re, _ := regexp.Compile(pattern)
			return func(doc bson.Raw) bool {
				rawVal, err := doc.LookupErr(keys...)
				if err != nil || rawVal.Type != bsontype.String {
					return false
				}
				return re.MatchString(rawVal.StringValue())
			}
		}
	}

	return func(doc bson.Raw) bool {
		return h.matchFilter(doc, filter)
	}
}

func (h *ConnectionHandler) matchFilter(itemBSON []byte, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}

	if andRaw, exists := filter[globalconst.OpAnd]; exists {
		var andConditions []any
		if ba, ok := andRaw.(bson.A); ok {
			andConditions = ba
		} else if arr, ok := andRaw.([]any); ok {
			andConditions = arr
		}

		for _, cond := range andConditions {
			condMap, isM := asMap(cond)
			if isM && condMap != nil && !h.matchFilter(itemBSON, condMap) {
				return false
			}
		}
		if andConditions != nil {
			return true
		}
	}

	if orRaw, exists := filter[globalconst.OpOr]; exists {
		var orConditions []any
		if ba, ok := orRaw.(bson.A); ok {
			orConditions = ba
		} else if arr, ok := orRaw.([]any); ok {
			orConditions = arr
		}

		for _, cond := range orConditions {
			condMap, isM := asMap(cond)
			if isM && condMap != nil && h.matchFilter(itemBSON, condMap) {
				return true
			}
		}
		if orConditions != nil {
			return false
		}
	}

	if notRaw, exists := filter[globalconst.OpNot]; exists {
		notCondition, isM := asMap(notRaw)
		if isM && notCondition != nil {
			return !h.matchFilter(itemBSON, notCondition)
		}
	}

	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if !fieldOk || !opOk {
		return false
	}

	keys := strings.Split(field, ".")
	rawVal, err := bson.Raw(itemBSON).LookupErr(keys...)

	itemValueExists := err == nil && rawVal.Type != bsontype.Null

	if op == globalconst.OpIsNull {
		return !itemValueExists
	}
	if op == globalconst.OpIsNotNull {
		return itemValueExists
	}

	if !itemValueExists {
		return false
	}

	var itemValue any
	switch rawVal.Type {
	case bsontype.String:
		itemValue = rawVal.StringValue()
	case bsontype.Int32:
		itemValue = rawVal.Int32()
	case bsontype.Int64:
		itemValue = rawVal.Int64()
	case bsontype.Double:
		itemValue = rawVal.Double()
	case bsontype.Boolean:
		itemValue = rawVal.Boolean()
	case bsontype.DateTime:
		itemValue = rawVal.Time()
	default:
		itemValue = rawVal.String()
	}

	switch op {
	case globalconst.OpEqual:
		return compare(itemValue, value) == 0
	case globalconst.OpNotEqual:
		return compare(itemValue, value) != 0
	case globalconst.OpGreaterThan:
		return compare(itemValue, value) > 0
	case globalconst.OpGreaterThanOrEqual:
		return compare(itemValue, value) >= 0
	case globalconst.OpLessThan:
		return compare(itemValue, value) < 0
	case globalconst.OpLessThanOrEqual:
		return compare(itemValue, value) <= 0
	case globalconst.OpLike:
		if sVal, isStr := itemValue.(string); isStr {
			if pattern, isStrPattern := value.(string); isStrPattern {
				pattern = strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
				pattern = "(?i)^" + pattern + "$"

				var re *regexp.Regexp
				if val, ok := regexCache.Load(pattern); ok {
					re = val.(*regexp.Regexp)
				} else {
					var err error
					re, err = regexp.Compile(pattern)
					if err != nil {
						slog.Warn("Error compiling LIKE regex", "pattern", pattern, "error", err)
						return false
					}
					regexCache.Store(pattern, re)
				}
				return re.MatchString(sVal)
			}
		}
		return false
	case globalconst.OpBetween:
		var bounds []any
		if ba, ok := value.(bson.A); ok {
			bounds = ba
		} else if arr, ok := value.([]any); ok {
			bounds = arr
		}

		if len(bounds) == 2 {
			val := compare(itemValue, bounds[0])
			val2 := compare(itemValue, bounds[1])
			return val >= 0 && val2 <= 0
		}
		return false
	case globalconst.OpIn:
		var valuesList []any
		if ba, ok := value.(bson.A); ok {
			valuesList = ba
		} else if arr, ok := value.([]any); ok {
			valuesList = arr
		}

		for _, v := range valuesList {
			if compare(itemValue, v) == 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func compare(a, b any) int {
	if tA, okA := a.(time.Time); okA {
		if tB, okB := b.(time.Time); okB {
			if tA.Before(tB) {
				return -1
			}
			if tA.After(tB) {
				return 1
			}
			return 0
		}
	}

	if intA, isIntA := a.(int64); isIntA {
		if intB, isIntB := b.(int64); isIntB {
			if intA < intB {
				return -1
			}
			if intA > intB {
				return 1
			}
			return 0
		}
	}

	if numA, okA := toFloat64(a); okA {
		if numB, okB := toFloat64(b); okB {
			if numA < numB {
				return -1
			}
			if numA > numB {
				return 1
			}
			return 0
		}
	}
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	return strings.Compare(strA, strB)
}

func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func (h *ConnectionHandler) performAggregations(items []struct {
	Key string
	Val bson.M
}, query *Query) (any, error) {
	groupedData := make(map[string][]bson.M)

	if len(query.GroupBy) == 0 {
		groupKey := "_no_group_"
		groupedData[groupKey] = make([]bson.M, 0, len(items))
		for _, item := range items {
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	} else {
		for _, item := range items {
			groupKeyParts := make([]string, len(query.GroupBy))
			for i, field := range query.GroupBy {
				if val, ok := getNestedValue(item.Val, field); ok && val != nil {
					groupKeyParts[i] = fmt.Sprintf("%v", val)
				} else {
					groupKeyParts[i] = "NULL"
				}
			}
			groupKey := strings.Join(groupKeyParts, "|")
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	}

	var aggregatedResults []bson.M
	for groupKey, groupItems := range groupedData {
		resultRow := make(bson.M)

		if len(query.GroupBy) > 0 {
			if groupKey != "_no_group_" {
				groupKeyValues := strings.Split(groupKey, "|")
				for i, field := range query.GroupBy {
					if i < len(groupKeyValues) {
						setNestedValue(resultRow, field, groupKeyValues[i])
					}
				}
			}
		}

		for aggName, agg := range query.Aggregations {
			var aggValue any
			var err error

			switch agg.Func {
			case globalconst.AggCount:
				if agg.Field == "*" {
					aggValue = len(groupItems)
				} else {
					count := 0
					for _, item := range groupItems {
						if _, ok := getNestedValue(item, agg.Field); ok {
							count++
						}
					}
					aggValue = count
				}
			case globalconst.AggSum, globalconst.AggAvg, globalconst.AggMin, globalconst.AggMax:
				numbers := []float64{}
				for _, item := range groupItems {
					if val, ok := getNestedValue(item, agg.Field); ok {
						if num, convertedOk := toFloat64(val); convertedOk {
							numbers = append(numbers, num)
						}
					}
				}

				if len(numbers) == 0 {
					aggValue = nil
					continue
				}

				switch agg.Func {
				case globalconst.AggSum:
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum
				case globalconst.AggAvg:
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum / float64(len(numbers))
				case globalconst.AggMin:
					minVal := numbers[0]
					for _, n := range numbers {
						if n < minVal {
							minVal = n
						}
					}
					aggValue = minVal
				case globalconst.AggMax:
					maxVal := numbers[0]
					for _, n := range numbers {
						if n > maxVal {
							maxVal = n
						}
					}
					aggValue = maxVal
				default:
					err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
				}
			default:
				err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
			}

			if err != nil {
				return nil, err
			}
			resultRow[aggName] = aggValue
		}

		if len(query.Having) > 0 {
			rowBytes, _ := bson.Marshal(resultRow)
			if h.matchFilter(rowBytes, query.Having) {
				aggregatedResults = append(aggregatedResults, resultRow)
			}
		} else {
			aggregatedResults = append(aggregatedResults, resultRow)
		}
	}

	return aggregatedResults, nil
}

func min(a, b int) int {
	return int(math.Min(float64(a), float64(b)))
}

func max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

func intersectKeys(keySets [][]string) []string {
	if len(keySets) == 0 {
		return []string{}
	}

	sort.Slice(keySets, func(i, j int) bool {
		return len(keySets[i]) < len(keySets[j])
	})

	intersectionMap := make(map[string]struct{}, len(keySets[0]))
	for _, key := range keySets[0] {
		intersectionMap[key] = struct{}{}
	}

	for i := 1; i < len(keySets); i++ {
		if len(intersectionMap) == 0 {
			break
		}
		currentSetMap := make(map[string]struct{})
		for _, key := range keySets[i] {
			if _, found := intersectionMap[key]; found {
				currentSetMap[key] = struct{}{}
			}
		}
		intersectionMap = currentSetMap
	}

	finalKeys := make([]string, 0, len(intersectionMap))
	for key := range intersectionMap {
		finalKeys = append(finalKeys, key)
	}

	return finalKeys
}

func getNestedValue(data map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = data

	for _, part := range parts {
		if currentSlice, ok := current.(bson.A); ok && len(currentSlice) == 1 {
			current = currentSlice[0]
		} else if currentSliceAny, ok := current.([]any); ok && len(currentSliceAny) == 1 {
			current = currentSliceAny[0]
		}

		currentMap, ok := current.(bson.M)
		if !ok {
			currentMapStd, okStd := current.(map[string]any)
			if !okStd {
				return nil, false
			}
			currentMap = currentMapStd
		}

		value, found := currentMap[part]
		if !found {
			return nil, false
		}
		current = value
	}

	return current, true
}

func setNestedValue(data map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	currentMap := data

	for i, key := range parts {
		if i == len(parts)-1 {
			currentMap[key] = value
			return
		}

		if _, ok := currentMap[key]; !ok {
			currentMap[key] = make(bson.M)
		}

		nextMap, ok := currentMap[key].(bson.M)
		if !ok {
			nextMapStd, okStd := currentMap[key].(map[string]any)
			if !okStd {
				return
			}
			nextMap = nextMapStd
		}
		currentMap = nextMap
	}
}

// --- ZERO-COPY HELPERS ---

func getRawValue(doc bson.Raw, path string) (any, bool) {
	val, err := doc.LookupErr(strings.Split(path, ".")...)
	if err != nil || val.Type == bsontype.Null {
		return nil, false
	}
	switch val.Type {
	case bsontype.Double:
		return val.Double(), true
	case bsontype.String:
		return val.StringValue(), true
	case bsontype.Int32:
		return int64(val.Int32()), true
	case bsontype.Int64:
		return val.Int64(), true
	case bsontype.Boolean:
		return val.Boolean(), true
	case bsontype.DateTime:
		return val.Time(), true
	default:
		return val.String(), true
	}
}
func getRawValueByParsedPath(doc bson.Raw, keys []string) (any, bool) {
	val, err := doc.LookupErr(keys...)
	if err != nil || val.Type == bsontype.Null {
		return nil, false
	}
	switch val.Type {
	case bsontype.Double:
		return val.Double(), true
	case bsontype.String:
		return val.StringValue(), true
	case bsontype.Int32:
		return int64(val.Int32()), true
	case bsontype.Int64:
		return val.Int64(), true
	case bsontype.Boolean:
		return val.Boolean(), true
	case bsontype.DateTime:
		return val.Time(), true
	default:
		return val.String(), true
	}
}

type SortItem struct {
	Raw    bson.Raw
	Values []any
}

type RawResultHeap struct {
	items   []SortItem
	orderBy []OrderByClause
}

func (h *RawResultHeap) Len() int      { return len(h.items) }
func (h *RawResultHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *RawResultHeap) Push(x any)    { h.items = append(h.items, x.(SortItem)) }
func (h *RawResultHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}
func (h *RawResultHeap) Less(i, j int) bool { return !h.LessItem(h.items[i], h.items[j]) }

func (h *RawResultHeap) LessItem(a, b SortItem) bool {
	for idx, ob := range h.orderBy {
		valA := a.Values[idx]
		valB := b.Values[idx]

		if valA == nil && valB == nil {
			continue
		}
		if valA == nil {
			return true
		}
		if valB == nil {
			return false
		}

		cmp := compare(valA, valB)
		if cmp != 0 {
			if ob.Direction == globalconst.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return false
}

func sortRawResults(results []SortItem, orderBy []OrderByClause) {
	sort.Slice(results, func(i, j int) bool {
		for idx, ob := range orderBy {
			valA := results[i].Values[idx]
			valB := results[j].Values[idx]

			if valA == nil && valB == nil {
				continue
			}
			if valA == nil {
				return true
			}
			if valB == nil {
				return false
			}

			cmp := compare(valA, valB)
			if cmp != 0 {
				if ob.Direction == globalconst.SortDesc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

func extractBsonRawValue(val bson.RawValue) any {
	switch val.Type {
	case bsontype.Double:
		return val.Double()
	case bsontype.String:
		return val.StringValue()
	case bsontype.Int32:
		return int64(val.Int32())
	case bsontype.Int64:
		return val.Int64()
	case bsontype.Boolean:
		return val.Boolean()
	case bsontype.DateTime:
		return val.Time()
	default:
		return nil
	}
}

// --- ESTRUCTURAS DE ACUMULACIÓN OPTIMIZADAS ---
type aggOp struct {
	Alias      string
	Func       string
	Field      string
	ParsedPath []string
}

type aggAccumulator struct {
	GroupValues []any
	Count       int
	Sums        []float64
	Mins        []float64
	Maxs        []float64
	Counts      []int
	HasVal      []bool
}

// performRawAggregations realiza agregaciones sobre un slice en memoria
func (h *ConnectionHandler) performRawAggregations(items []bson.Raw, query *Query) (any, error) {
	var ops []aggOp
	for alias, agg := range query.Aggregations {
		var parsed []string
		if agg.Field != "*" {
			parsed = strings.Split(agg.Field, ".")
		}
		ops = append(ops, aggOp{Alias: alias, Func: agg.Func, Field: agg.Field, ParsedPath: parsed})
	}

	parsedGroupPaths := make([][]string, len(query.GroupBy))
	for i, field := range query.GroupBy {
		parsedGroupPaths[i] = strings.Split(field, ".")
	}

	groups := make(map[string]*aggAccumulator)
	keyBuffer := make([]byte, 0, 128)

	for _, item := range items {
		keyBuffer = keyBuffer[:0]

		if len(query.GroupBy) == 0 {
			keyBuffer = append(keyBuffer, "_no_group_"...)
		} else {
			for i := range query.GroupBy {
				if i > 0 {
					keyBuffer = append(keyBuffer, '|')
				}
				val, err := item.LookupErr(parsedGroupPaths[i]...)
				if err == nil && val.Type != bsontype.Null {
					keyBuffer = append(keyBuffer, byte(val.Type))
					keyBuffer = append(keyBuffer, val.Value...)
				} else {
					keyBuffer = append(keyBuffer, 0)
				}
			}
		}

		// 🔥 ZERO-ALLOCATION FAST PATH: El compilador de Go no asigna memoria para esto.
		acc, exists := groups[string(keyBuffer)]

		if !exists {
			// Solo creamos el string permanente si el grupo es nuevo
			groupStrKey := string(keyBuffer)
			groupVals := make([]any, len(query.GroupBy))

			if len(query.GroupBy) > 0 {
				for i := range query.GroupBy {
					val, err := item.LookupErr(parsedGroupPaths[i]...)
					if err == nil && val.Type != bsontype.Null {
						groupVals[i] = extractBsonRawValue(val)
					}
				}
			}

			acc = &aggAccumulator{
				GroupValues: groupVals,
				Sums:        make([]float64, len(ops)),
				Mins:        make([]float64, len(ops)),
				Maxs:        make([]float64, len(ops)),
				Counts:      make([]int, len(ops)),
				HasVal:      make([]bool, len(ops)),
			}
			groups[groupStrKey] = acc
		}

		acc.Count++

		for i, op := range ops {
			if op.Field == "*" {
				if op.Func == globalconst.AggCount {
					acc.Counts[i]++
				}
				continue
			}

			val, err := item.LookupErr(op.ParsedPath...)
			if err != nil || val.Type == bsontype.Null {
				continue
			}

			acc.Counts[i]++
			if op.Func == globalconst.AggCount {
				continue
			}

			// 🔥 MATEMÁTICA SIN REFLEXIÓN (Directo de los bytes)
			var num float64
			isNum := true
			switch val.Type {
			case bsontype.Double:
				num = val.Double()
			case bsontype.Int32:
				num = float64(val.Int32())
			case bsontype.Int64:
				num = float64(val.Int64())
			default:
				isNum = false
			}

			if !isNum {
				continue
			}

			switch op.Func {
			case globalconst.AggSum, globalconst.AggAvg:
				acc.Sums[i] += num
			case globalconst.AggMin:
				if !acc.HasVal[i] || num < acc.Mins[i] {
					acc.Mins[i] = num
					acc.HasVal[i] = true
				}
			case globalconst.AggMax:
				if !acc.HasVal[i] || num > acc.Maxs[i] {
					acc.Maxs[i] = num
					acc.HasVal[i] = true
				}
			}
		}
	}

	var aggregatedResults []bson.M
	for groupKey, acc := range groups {
		resultRow := make(bson.M)

		if len(query.GroupBy) > 0 && groupKey != "_no_group_" {
			for i, field := range query.GroupBy {
				setNestedValue(resultRow, field, acc.GroupValues[i])
			}
		}

		for i, op := range ops {
			var finalVal any
			switch op.Func {
			case globalconst.AggCount:
				finalVal = acc.Counts[i]
			case globalconst.AggSum:
				if acc.HasVal[i] || acc.Counts[i] > 0 {
					finalVal = acc.Sums[i]
				} else {
					finalVal = nil
				}
			case globalconst.AggAvg:
				if acc.Counts[i] > 0 {
					finalVal = acc.Sums[i] / float64(acc.Counts[i])
				} else {
					finalVal = nil
				}
			case globalconst.AggMin:
				if acc.HasVal[i] {
					finalVal = acc.Mins[i]
				} else {
					finalVal = nil
				}
			case globalconst.AggMax:
				if acc.HasVal[i] {
					finalVal = acc.Maxs[i]
				} else {
					finalVal = nil
				}
			}
			resultRow[op.Alias] = finalVal
		}

		if len(query.Having) > 0 {
			rowBytes, _ := bson.Marshal(resultRow)
			if h.matchFilter(rowBytes, query.Having) {
				aggregatedResults = append(aggregatedResults, resultRow)
			}
		} else {
			aggregatedResults = append(aggregatedResults, resultRow)
		}
	}

	return aggregatedResults, nil
}

func asMap(val any) (bson.M, bool) {
	if m, ok := val.(bson.M); ok {
		return m, true
	}
	if m, ok := val.(map[string]any); ok {
		return m, true
	}
	if d, ok := val.(bson.D); ok {
		return d.Map(), true
	}
	return nil, false
}

// streamAggregations realiza matemáticas directamente sobre el flujo del disco sin almacenar arrays.
func (h *ConnectionHandler) streamAggregations(colStore store.DataStore, query *Query) (any, error) {
	var ops []aggOp
	for alias, agg := range query.Aggregations {
		var parsed []string
		if agg.Field != "*" {
			parsed = strings.Split(agg.Field, ".")
		}
		ops = append(ops, aggOp{Alias: alias, Func: agg.Func, Field: agg.Field, ParsedPath: parsed})
	}

	parsedGroupPaths := make([][]string, len(query.GroupBy))
	for i, field := range query.GroupBy {
		parsedGroupPaths[i] = strings.Split(field, ".")
	}

	groups := make(map[string]*aggAccumulator)
	keyBuffer := make([]byte, 0, 128)

	// Leemos directo del disco con Zero-Copy y procesamos al instante
	colStore.StreamAll(func(k string, vBytes []byte) bool {
		item := bson.Raw(vBytes)
		keyBuffer = keyBuffer[:0]

		if len(query.GroupBy) == 0 {
			keyBuffer = append(keyBuffer, "_no_group_"...)
		} else {
			for i := range query.GroupBy {
				if i > 0 {
					keyBuffer = append(keyBuffer, '|')
				}
				val, err := item.LookupErr(parsedGroupPaths[i]...)
				if err == nil && val.Type != bsontype.Null {
					keyBuffer = append(keyBuffer, byte(val.Type))
					keyBuffer = append(keyBuffer, val.Value...)
				} else {
					keyBuffer = append(keyBuffer, 0)
				}
			}
		}

		// 🔥 ZERO-ALLOCATION FAST PATH
		acc, exists := groups[string(keyBuffer)]

		if !exists {
			groupStrKey := string(keyBuffer) // Solo asignamos RAM 1 vez por grupo
			groupVals := make([]any, len(query.GroupBy))
			if len(query.GroupBy) > 0 {
				for i := range query.GroupBy {
					if val, err := item.LookupErr(parsedGroupPaths[i]...); err == nil && val.Type != bsontype.Null {
						groupVals[i] = extractBsonRawValue(val)
					}
				}
			}

			acc = &aggAccumulator{
				GroupValues: groupVals,
				Sums:        make([]float64, len(ops)),
				Mins:        make([]float64, len(ops)),
				Maxs:        make([]float64, len(ops)),
				Counts:      make([]int, len(ops)),
				HasVal:      make([]bool, len(ops)),
			}
			groups[groupStrKey] = acc
		}

		acc.Count++

		for i, op := range ops {
			if op.Field == "*" {
				if op.Func == globalconst.AggCount {
					acc.Counts[i]++
				}
				continue
			}

			val, err := item.LookupErr(op.ParsedPath...)
			if err != nil || val.Type == bsontype.Null {
				continue
			}

			acc.Counts[i]++
			if op.Func == globalconst.AggCount {
				continue
			}

			// 🔥 MATEMÁTICA SIN REFLEXIÓN (Directo de los bytes)
			var num float64
			isNum := true
			switch val.Type {
			case bsontype.Double:
				num = val.Double()
			case bsontype.Int32:
				num = float64(val.Int32())
			case bsontype.Int64:
				num = float64(val.Int64())
			default:
				isNum = false
			}

			if !isNum {
				continue
			}

			switch op.Func {
			case globalconst.AggSum, globalconst.AggAvg:
				acc.Sums[i] += num
			case globalconst.AggMin:
				if !acc.HasVal[i] || num < acc.Mins[i] {
					acc.Mins[i] = num
					acc.HasVal[i] = true
				}
			case globalconst.AggMax:
				if !acc.HasVal[i] || num > acc.Maxs[i] {
					acc.Maxs[i] = num
					acc.HasVal[i] = true
				}
			}
		}
		return true
	})

	var aggregatedResults []bson.M
	for groupKey, acc := range groups {
		resultRow := make(bson.M)

		if len(query.GroupBy) > 0 && groupKey != "_no_group_" {
			for i, field := range query.GroupBy {
				setNestedValue(resultRow, field, acc.GroupValues[i])
			}
		}

		for i, op := range ops {
			var finalVal any
			switch op.Func {
			case globalconst.AggCount:
				finalVal = acc.Counts[i]
			case globalconst.AggSum:
				if acc.HasVal[i] || acc.Counts[i] > 0 {
					finalVal = acc.Sums[i]
				} else {
					finalVal = nil
				}
			case globalconst.AggAvg:
				if acc.Counts[i] > 0 {
					finalVal = acc.Sums[i] / float64(acc.Counts[i])
				} else {
					finalVal = nil
				}
			case globalconst.AggMin:
				if acc.HasVal[i] {
					finalVal = acc.Mins[i]
				} else {
					finalVal = nil
				}
			case globalconst.AggMax:
				if acc.HasVal[i] {
					finalVal = acc.Maxs[i]
				} else {
					finalVal = nil
				}
			}
			resultRow[op.Alias] = finalVal
		}

		if len(query.Having) > 0 {
			rowBytes, _ := bson.Marshal(resultRow)
			if h.matchFilter(rowBytes, query.Having) {
				aggregatedResults = append(aggregatedResults, resultRow)
			}
		} else {
			aggregatedResults = append(aggregatedResults, resultRow)
		}
	}

	return aggregatedResults, nil
}
