package solanaconfirm

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	var _chunks = make([][]T, 0, (len(items)/chunkSize)+1)
	for chunkSize < len(items) {
		items, _chunks = items[chunkSize:], append(_chunks, items[0:chunkSize:chunkSize])
	}
	return append(_chunks, items)
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}
