package transport

type lastMileFake struct{}

func newLastMileFake(w *world) *lastMileFake { return &lastMileFake{} }
