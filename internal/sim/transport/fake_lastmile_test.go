package transport

import "time"

type lastMileFake struct{}

func newLastMileFake(w *world) *lastMileFake { return &lastMileFake{} }

func (l *lastMileFake) onLoadArrived(station string, tns []string, at time.Time) {}
