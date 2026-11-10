package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// editMu serialises EditSymbols calls within the process.
var editMu sync.Mutex

// EditSymbols rewrites the top-level `symbols` list of the YAML file at path.
// fn receives the list as it is on disk and returns the new one. Only the
// lines of the `symbols` entry are replaced (with a one-line flow list), so
// every other line and comment of the file is kept byte for byte. The result
// is validated like any config before it is written; an invalid result is
// returned as an error and the file is left untouched.
//
// The file is replaced atomically (temp file + rename in the same
// directory), falling back to an in-place write when the directory is not
// writable. Either way config.Watch sees the change and hot reloads it.
func EditSymbols(path string, fn func(cur []string) ([]string, error)) (*Config, error) {
	editMu.Lock()
	defer editMu.Unlock()

	target, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cur, err := fileSymbols(data)
	if err != nil {
		return nil, err
	}
	next, err := fn(cur)
	if err != nil {
		return nil, err
	}
	out, err := replaceSymbols(data, next)
	if err != nil {
		return nil, err
	}
	c, err := Parse(out)
	if err != nil {
		return nil, err
	}
	if got, _ := fileSymbols(out); !equalStrings(got, next) {
		// Should never happen; refuse to write rather than corrupt the file.
		return nil, fmt.Errorf("edit symbols: rewritten file holds %v, want %v", got, next)
	}
	if err := writeFileAtomic(target, out); err != nil {
		return nil, err
	}
	return c, nil
}

// FileSymbols returns the top-level `symbols` list of the YAML file at path,
// exactly as written there.
func FileSymbols(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return fileSymbols(data)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// rootMapping parses data and returns its top-level block mapping.
func rootMapping(data []byte) (*yaml.Node, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, errors.New("edit symbols: config file is empty")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, errors.New("edit symbols: config root is not a mapping")
	}
	if root.Style&yaml.FlowStyle != 0 {
		return nil, errors.New("edit symbols: config root written as a {flow} mapping is not supported")
	}
	return root, nil
}

func fileSymbols(data []byte) ([]string, error) {
	root, err := rootMapping(data)
	if err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "symbols" {
			continue
		}
		v := root.Content[i+1]
		switch {
		case v.Kind == yaml.ScalarNode && (v.Tag == "!!null" || v.Value == ""):
			return nil, nil
		case v.Kind != yaml.SequenceNode:
			return nil, fmt.Errorf("edit symbols: line %d: symbols is not a list", v.Line)
		}
		var out []string
		if err := v.Decode(&out); err != nil {
			return nil, fmt.Errorf("edit symbols: %w", err)
		}
		return out, nil
	}
	return nil, nil
}

// replaceSymbols returns data with the top-level `symbols` entry replaced by
// `symbols: [A, B]`, or with that line inserted before the first top-level
// key when there is none.
func replaceSymbols(data []byte, syms []string) ([]byte, error) {
	for _, s := range syms {
		if s == "" || strings.ContainsAny(s, " \t,[]{}#:'\"&*!|>%@`\r\n") {
			return nil, fmt.Errorf("edit symbols: invalid symbol %q", s)
		}
	}
	root, err := rootMapping(data)
	if err != nil {
		return nil, err
	}
	if len(root.Content) == 0 {
		return nil, errors.New("edit symbols: config root is empty")
	}
	eol := "\n"
	if bytes.Contains(data, []byte("\r\n")) {
		eol = "\r\n"
	}
	lines := strings.SplitAfter(string(data), "\n")

	keyIdx := -1
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "symbols" {
			keyIdx = i
			break
		}
	}
	var start, end int // 0-based line range [start, end) to replace
	var indent string
	if keyIdx < 0 {
		first := root.Content[0]
		start, end = first.Line-1, first.Line-1
		indent = strings.Repeat(" ", first.Column-1)
	} else {
		key := root.Content[keyIdx]
		start = key.Line - 1
		indent = strings.Repeat(" ", key.Column-1)
		end = len(lines)
		if keyIdx+2 < len(root.Content) {
			end = root.Content[keyIdx+2].Line - 1
		}
		// Keep blank lines and comments (not indented past the key) that
		// precede the next key: they belong to it, not to the symbols list.
		for end > start+1 {
			t := strings.TrimRight(lines[end-1], "\r\n")
			trim := strings.TrimLeft(t, " \t")
			if trim != "" && (!strings.HasPrefix(trim, "#") || len(t)-len(trim) > key.Column-1) {
				break
			}
			end--
		}
		// A file without a trailing newline: the last replaced line had none.
		if end == len(lines) && !strings.HasSuffix(lines[end-1], "\n") {
			eol = ""
		}
	}
	if start < 0 || start > len(lines) || end < start || end > len(lines) {
		return nil, errors.New("edit symbols: cannot locate the symbols entry")
	}
	newLine := indent + "symbols: [" + strings.Join(syms, ", ") + "]" + eol
	var b strings.Builder
	for _, l := range lines[:start] {
		b.WriteString(l)
	}
	b.WriteString(newLine)
	for _, l := range lines[end:] {
		b.WriteString(l)
	}
	return []byte(b.String()), nil
}

// writeFileAtomic replaces path with data, keeping its permission bits.
func writeFileAtomic(path string, data []byte) error {
	mode := os.FileMode(0o644)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	dir, base := filepath.Split(path)
	f, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		// Directory not writable (e.g. only the file itself is): write in place.
		if werr := os.WriteFile(path, data, mode); werr != nil {
			return fmt.Errorf("write config: %w", werr)
		}
		return nil
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	ok = true
	return nil
}
