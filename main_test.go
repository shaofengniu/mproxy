package main

import (
	"fmt"
	"testing"

	"github.com/bmizerany/mc"
)

func TestSet(t *testing.T) {
	sc, err := mc.Dial("tcp", ":11211")
	if err != nil {
		t.Error(err)
	}

	err = sc.Del("foo")
	if err != nil && err != mc.ErrNotFound {
		t.Error(err)
	}

	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		t.Error(err)
	}

	err = pc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		t.Error(err)
	}

	val, _, _, err := sc.Get("foo")
	if err != nil {
		t.Error(err)
	}

	if val != "bar" {
		err = fmt.Errorf("result not match: %s", val)
		t.Error(err)
	}
}

func BenchmarkSet(b *testing.B) {
	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Set("foo", "bar", 0, 0, 0)
	}
}

func TestGet(t *testing.T) {
	sc, err := mc.Dial("tcp", ":11211")
	if err != nil {
		t.Error(err)
	}
	err = sc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		t.Error(err)
	}

	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		t.Error(err)
	}

	val, _, _, err := pc.Get("foo")
	if err != nil {
		t.Error(err)
	}

	if val != "bar" {
		t.Error(err)
	}
}

func BenchmarkGet(b *testing.B) {
	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Get("foo")
	}
}

func TestDelete(t *testing.T) {
	sc, err := mc.Dial("tcp", ":11211")
	if err != nil {
		t.Error(err)
	}
	err = sc.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		err = fmt.Errorf("asf")
		t.Error(err)
	}

	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		t.Error(err)
	}

	err = pc.Del("foo")
	if err != nil && err != mc.ErrNotFound {
		err = fmt.Errorf("asf")
		t.Error(err)
	}

	_, _, _, err = sc.Get("foo")
	if err != mc.ErrNotFound {
		err = fmt.Errorf("failed to delete foo")
		t.Error(err)
	}
}

func BenchmarkDelete(b *testing.B) {
	pc, err := mc.Dial("tcp", ":8080")
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Del("foo")
	}
}
