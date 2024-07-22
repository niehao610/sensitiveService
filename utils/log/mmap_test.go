package log

import "testing"

func BenchmarkLog(b *testing.B) {
	//200000              8478 ns/op             296 B/op          7 allocs/op		55.81MB
	//500000              2705 ns/op             456 B/op          9 allocs/op		207.09MB
	b.Run("benchlog1", func(b *testing.B) {
		LoggerSetRollingDaily("./", "benchlog1.log", "")
		SetConsole(false)
		for i := 0; i < b.N; i++ {
			Info("hello world")
		}
	})
}
