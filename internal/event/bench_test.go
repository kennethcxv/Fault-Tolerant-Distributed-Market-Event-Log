package event

import "testing"

func BenchmarkEncode(b *testing.B) {
	e := sample()
	e.Payload = make([]byte, 64)
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf = e.AppendTo(buf[:0])
	}
}

func BenchmarkDecode(b *testing.B) {
	e := sample()
	e.Payload = make([]byte, 64)
	rec := e.AppendTo(nil)
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(rec); err != nil {
			b.Fatal(err)
		}
	}
}
