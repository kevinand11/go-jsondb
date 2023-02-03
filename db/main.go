package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const (
	Version = "0.0.0"
	nestPath = "./.data/"
)

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		name string
		log Logger
	}
	collection struct {
		name string
		driver *Driver
	}
	Options struct {
		Logger
	}
)


func stat (path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) { fi, err = os.Stat(path + ".json") }
	return fi, err
}

func New (name string, options *Options) (*Driver, error) {
	if name == "" { name = "test" }
	dir := filepath.Clean(nestPath + name)
	opts := Options{}
	if options != nil { opts = *options }
	if opts.Logger == nil { opts.Logger = lumber.NewConsoleLogger(lumber.INFO) }

	driver := Driver {
		name: name,
		mutexes: make(map[string]*sync.Mutex),
		log: opts.Logger,
	}

	if _, err := os.Stat(dir); err != nil { return &driver, nil }

	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Collection (name string) (*collection, error) {
	if name == "" { return nil, fmt.Errorf("missing collection") }

	d.mutex.Lock()
    defer d.mutex.Unlock()

	path := filepath.Join(d.name, name)

	c := collection {
		name: name,
		driver: d,
	}

	if _, err := os.Stat(path); err != nil { return &c, nil }

	os.MkdirAll(path, 0755)
	return &c, nil
}

func (c *collection) path () string {
	return filepath.Join(nestPath, c.driver.name, c.name)
}

func (c *collection) Write (resource string, v interface {}) error {
	if resource == "" { return fmt.Errorf("missing resource") }

	mutex := c.getOrCreateMutex()
	mutex.Lock()
    defer mutex.Unlock()

	dir := c.path()
	filePath := filepath.Join(dir, resource + ".json")
	tmpPath := filePath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil { return err }

	b, err := json.MarshalIndent(v, "", "\t")
	if err!= nil { return err }
	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil { return err }

	return os.Rename(tmpPath, filePath)
}

func (c *collection) Read (resource string) (string, error) {
	if resource == "" { return "", fmt.Errorf("missing resource") }

	filePath := filepath.Join(c.path(), resource)
	if _, err := stat(filePath); err != nil { return "", err }

	b, err := os.ReadFile(filePath + ".json")
	if err != nil { return "", err }

	return string(b), nil
}

func (c *collection) ReadAll () (records []string, err error) {
	dir := c.path()
	if _, err := stat(dir); err != nil { return records, err }

	files, _ := os.ReadDir(dir)

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil { return records, err }

		records = append(records, string(b))
	}

	return records, err
}

func (c *collection) Delete (resource string) error {
	mutex := c.getOrCreateMutex()
	mutex.Lock()
    defer mutex.Unlock()

	filePath := filepath.Join(c.path(), resource + ".json")

	switch fi, err := stat(filePath); {
		case fi == nil, err != nil:
			return fmt.Errorf("unable to find resource %s", resource)
		case fi.Mode().IsRegular():
			return os.RemoveAll(filePath)
	}

	return nil
}

func (c *collection) getOrCreateMutex () *sync.Mutex {
	c.driver.mutex.Lock()
	defer c.driver.mutex.Unlock()

	m, ok := c.driver.mutexes[c.name]

	if !ok {
		m = &sync.Mutex{}
		c.driver.mutexes[c.name] = m
	}

	return m
}

func ParseOne[K interface {}] (r string, v K) (K, error) {
	rec := new(K)
	err := json.Unmarshal([]byte(r), &rec)
	if err != nil { return *rec, fmt.Errorf("error %v", err) }
	return *rec, nil
}

func ParseMany[K interface {}] (data []string, v K) ([]K, error) {
	records := make([]K, len(data))
	for idx, record := range data {
		rec := new(K)
		err := json.Unmarshal([]byte(record), &rec)
		if err != nil { return nil, fmt.Errorf("error %v", err) }
		records[idx] = *rec
	}
	return records, nil
}