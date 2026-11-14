package shopper

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// ErrInDoubt is returned by Confirm when the bounded wait ended with
// the session still AUTHORIZING or PLACING: the shopper does not know whether it paid.
var ErrInDoubt = errors.New("shopper: placement still in doubt after the bounded wait")

// Rejection is a step the BFF (or the network, or the stack behind it) refused: a finding, not a
// panic. Status 0 is a transport error.
type Rejection struct {
	Route  string // templated route, e.g. "POST /carts/{id}/lines"
	Status int
	Type   string // the problem type, e.g. "/problems/cancel-after-lock"
	Title  string
	Detail string
	Body   string // the raw body, truncated
}

// Error implements error.
func (r *Rejection) Error() string {
	if r.Status == 0 {
		return fmt.Sprintf("%s -> transport error: %s", r.Route, r.Detail)
	}
	s := fmt.Sprintf("%s -> %d", r.Route, r.Status)
	if slug := r.Slug(); slug != "" {
		s += " " + slug
	}
	if r.Detail != "" {
		s += ": " + r.Detail
	} else if r.Title != "" {
		s += ": " + r.Title
	}
	return s
}

// Slug is the problem type's last element ("cancel-after-lock"); empty when the body was no problem.
func (r *Rejection) Slug() string { return problemBody{Type: r.Type}.slug() }

// Is reports whether err is a *Rejection of the given problem slug.
func Is(err error, slug string) bool {
	var r *Rejection
	return errors.As(err, &r) && r.Slug() == slug
}

// rejection builds the Rejection of a non-2xx answer.
func rejection(route string, r sim.Resp) *Rejection {
	rej := &Rejection{Route: route, Status: r.Status}
	if r.Status == 0 {
		rej.Detail = string(r.Body)
		return rej
	}
	rej.Body = sim.Truncate(string(r.Body), 300)
	var p problemBody
	if json.Unmarshal(r.Body, &p) == nil && p.Type != "" {
		rej.Type, rej.Title, rej.Detail = p.Type, p.Title, p.Detail
	}
	return rej
}

// finding records one rejected request in the shopper's findings.
func (s *Shopper) finding(route string, r sim.Resp) {
	rej := rejection(route, r)
	line := rej.Error()
	if rej.Type == "" && rej.Body != "" {
		line += " " + strings.TrimSpace(rej.Body)
	}
	s.findings = append(s.findings, line)
}

// decode returns the answer's body decoded into out, or the Rejection of a non-2xx answer. A 2xx
// that does not decode is a contract violation: it is a finding too.
func (s *Shopper) decode(route string, rp reply, out any) error {
	if !rp.OK() {
		return rejection(route, rp.Resp)
	}
	if err := rp.Decode(out); err != nil {
		rej := &Rejection{Route: route, Status: rp.Status, Detail: "contract violation: " + err.Error()}
		s.findings = append(s.findings, rej.Error())
		return rej
	}
	return nil
}
