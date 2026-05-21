package gogo

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMultipartPartSaveIntoRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "avatar.txt")
	if err := os.WriteFile(path, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	part := &MultipartPart{FileName: "avatar.txt", Data: []byte("replace me")}
	if _, err := part.SaveInto(dir); err == nil {
		t.Fatal("SaveInto succeeded over an existing file")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed file: %v", err)
	}
	if string(got) != "keep me" {
		t.Fatalf("existing file was overwritten: %q", got)
	}
}

func TestMultipartPartSaveAtNewRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "avatar.txt")
	if err := os.WriteFile(path, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	part := &MultipartPart{Data: []byte("replace me")}
	if err := part.SaveAtNew(path); err == nil {
		t.Fatal("SaveAtNew succeeded over an existing file")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed file: %v", err)
	}
	if string(got) != "keep me" {
		t.Fatalf("existing file was overwritten: %q", got)
	}
}

func TestMultipartPartSaveAtNewRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, []byte("target"), 0o644); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	part := &MultipartPart{Data: []byte("replace")}
	if err := part.SaveAtNew(link); err == nil {
		t.Fatal("SaveAtNew succeeded over a symlink")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "target" {
		t.Fatalf("symlink target was overwritten: %q", got)
	}
}

func TestMultipartPartSaveIntoRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, []byte("target"), 0o644); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	part := &MultipartPart{FileName: "link.txt", Data: []byte("replace")}
	if _, err := part.SaveInto(dir); err == nil {
		t.Fatal("SaveInto succeeded over a symlink")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "target" {
		t.Fatalf("symlink target was overwritten: %q", got)
	}
}

func TestMultipartStreamPartSaveIntoRemovesPartialOnCopyError(t *testing.T) {
	dir := t.TempDir()
	part := &MultipartStreamPart{
		FileName: "partial.txt",
		Reader:   &errorAfterReader{data: []byte("partial")},
	}
	if _, err := part.SaveInto(dir); err == nil {
		t.Fatal("SaveInto succeeded despite reader error")
	}
	if _, err := os.Stat(filepath.Join(dir, "partial.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial file still exists, stat err=%v", err)
	}
}

type errorAfterReader struct {
	data []byte
	done bool
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errors.New("boom")
	}
	r.done = true
	return copy(p, r.data), nil
}
