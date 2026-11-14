package inbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRegisterASNAnnouncesTheLinesSortedAndNumberedFromOne(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	// Duplicates merge, a zero quantity is dropped, and the order does not depend on how the caller listed them.
	asn, err := c.RegisterASN(context.Background(), []Line{{skuC, 5}, {skuA, 10}, {skuB, 0}, {skuA, 7}})
	if err != nil {
		t.Fatalf("RegisterASN: %v", err)
	}
	want := []Line{{skuA, 17}, {skuC, 5}}
	if len(asn.Lines) != 2 || asn.Lines[0] != want[0] || asn.Lines[1] != want[1] {
		t.Fatalf("ASN lines = %v, want %v", asn.Lines, want)
	}
	if !strings.HasPrefix(asn.Number, "ASN-T1-IB-") {
		t.Errorf("ASN number %q is not run-scoped and inbound-scoped", asn.Number)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	rec := w.recv.asns[asn.Number]
	if rec == nil || rec.state != "Registered" || rec.supplier != "SIM-SUPPLIER" || len(rec.lines) != 2 {
		t.Fatalf("inbound-receiving holds %+v, want a Registered ASN of SIM-SUPPLIER with 2 lines", rec)
	}
}

func TestRegisterASNRetriesProductMasterLagWithFreshKeys(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	w.recv.skuLag[skuA] = &lag{n: 3}
	if _, err := c.RegisterASN(context.Background(), []Line{{skuA, 10}}); err != nil {
		t.Fatalf("RegisterASN: %v", err)
	}
	if n := w.served("inbound-receiving POST /asns"); n != 4 {
		t.Errorf("%d POST /asns, want 4 (3 unknown-sku refusals then the registration)", n)
	}
	// the world's cleanup asserts every key was used once
}

func TestRegisterASNOfAnUnknownSkuTimesOutAndSaysWhoMustKnowIt(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, err := c.RegisterASN(context.Background(), []Line{{"SKU-NOPE", 1}})
	if err == nil || !IsTimeout(err) {
		t.Fatalf("err = %v, want a bounded timeout", err)
	}
	for _, want := range []string{"unknown-sku", "SKU-NOPE", "product-master"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestRegisterASNRefusalOtherThanLagIsNotRetried(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	w.recv.asns["ASN-T1-IB-001"] = &asnRec{number: "ASN-T1-IB-001", state: "Registered"}
	_, err := c.RegisterASN(context.Background(), []Line{{skuA, 10}})
	if err == nil || !strings.Contains(err.Error(), "409 asn-already-exists") {
		t.Fatalf("err = %v, want 409 asn-already-exists", err)
	}
	if n := w.served("inbound-receiving POST /asns"); n != 1 {
		t.Errorf("%d POST /asns, want exactly 1", n)
	}
}

func TestRegisterASNWithNothingToAnnounceSendsNothing(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	if _, err := c.RegisterASN(context.Background(), []Line{{skuA, 0}, {"", 4}}); err == nil {
		t.Fatal("RegisterASN of no lines returned nil")
	}
	if n := w.writes(); n != 0 {
		t.Errorf("%d writes, want 0", n)
	}
}

func TestEveryASNNumberIsDistinct(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		asn, err := c.RegisterASN(context.Background(), []Line{{skuA, 1}})
		if err != nil {
			t.Fatal(err)
		}
		if seen[asn.Number] {
			t.Fatalf("ASN number %s minted twice", asn.Number)
		}
		seen[asn.Number] = true
	}
}

func TestRegisterASNSendsTheWireNamesOfTheContract(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	if _, err := c.RegisterASN(context.Background(), []Line{{skuA, 10}}); err != nil {
		t.Fatal(err)
	}
	// The fake's strict validation already refuses unknown shapes; this pins the body the services read.
	var body map[string]any
	for _, p := range w.bodies("inbound-receiving POST /asns") {
		if err := json.Unmarshal(p, &body); err != nil {
			t.Fatal(err)
		}
	}
	lines, _ := body["lines"].([]any)
	first, _ := lines[0].(map[string]any)
	if body["asnNumber"] == nil || body["supplierRef"] != "SIM-SUPPLIER" || first["lineNo"] != float64(1) || first["sku"] != skuA || first["expectedQty"] != float64(10) {
		t.Errorf("body = %v", body)
	}
}
