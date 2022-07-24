package persistence

func ItemsToAnySlice[T any](items []T) []any {
	ln := len(items)
	result := make([]any, ln, ln)
	for i := range items {
		result[i] = items[i]
	}
	return result
}
