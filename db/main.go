package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

func (c *collection) Read (resource string, r any) error {
	if resource == "" { return fmt.Errorf("missing resource") }

	filePath := filepath.Join(c.path(), resource)
	if _, err := stat(filePath); err != nil { return err }

	b, err := os.ReadFile(filePath + ".json")
	if err != nil { return err }

	return json.Unmarshal(b, &r)
}

func (c *collection) ReadAll (r any) error {
	dir := c.path()
	if _, err := stat(dir); err != nil { return err }

	files, _ := os.ReadDir(dir)

	resultsVal := reflect.ValueOf(r)
	if resultsVal.Kind() != reflect.Ptr { return fmt.Errorf("results argument must be a pointer to a slice") }
	sliceVal := resultsVal.Elem()
	if sliceVal.Kind() == reflect.Interface { sliceVal = sliceVal.Elem() }
	if sliceVal.Kind() != reflect.Slice { return fmt.Errorf("results argument must be a pointer to a slice") }
	elementType := sliceVal.Type().Elem()

	for index, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil { return err }

		if sliceVal.Len() == index {
			// slice is full
			newElem := reflect.New(elementType)
			sliceVal = reflect.Append(sliceVal, newElem.Elem())
			sliceVal = sliceVal.Slice(0, sliceVal.Cap())
		}

		currElem := sliceVal.Index(index).Addr().Interface()
		if err = json.Unmarshal(b, currElem); err != nil { return err }
	}

	resultsVal.Elem().Set(sliceVal.Slice(0, len(files)))

	return nil
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