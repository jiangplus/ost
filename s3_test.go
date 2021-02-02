package main

import (
	"testing"
)

func TestGetPath(t *testing.T) {
	t.Run("getPath for prefixed string", func(t *testing.T) {
		got := getPath("/abc")
		want := "abc"

		if got != want {
			t.Errorf("got '%q' want '%q'", got, want)
		}
	})

	t.Run("getPath for non-prefixed string", func(t *testing.T) {
		got := getPath("abc")
		want := "abc"

		if got != want {
			t.Errorf("got '%q' want '%q'", got, want)
		}
	})

	t.Run("getPath for empty string", func(t *testing.T) {
		got := getPath("")
		want := ""

		if got != want {
			t.Errorf("got '%q' want '%q'", got, want)
		}
	})
}
