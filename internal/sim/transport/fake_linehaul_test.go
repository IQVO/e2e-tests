package transport

type linehaulFake struct{}

func newLinehaulFake(w *world) *linehaulFake { return &linehaulFake{} }
