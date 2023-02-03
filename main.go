package main

import (
	"encoding/json"
	"flag"
	"fmt"

	database "github.com/kevinand11/go-jsondb/db"
)


type User struct {
	Name string
	Age json.Number
}

func main () {
	dbName := flag.String("db", "test", "a string")
	flag.Parse()
	db, err := database.New(*dbName, nil)
	if err != nil { fmt.Println("Error", err) }
	col, err := db.Collection("users")
	if err != nil { fmt.Println("Error", err) }

	employees := []User {
		{ Name: "John", Age: json.Number("30") },
		{ Name: "Mary", Age: json.Number("25") },
		{ Name: "Peter", Age: json.Number("20") },
		{ Name: "Mike", Age: json.Number("15") },
		{ Name: "Jack", Age: json.Number("10") },
		{ Name: "Jill", Age: json.Number("5") },
	}

	for _, employee := range employees {
        col.Write(employee.Name, employee)
	}

	record, err := col.Read(employees[0].Name)
	if (err != nil) { fmt.Println("Error", err) }
	fmt.Println()
	user, err := database.ParseOne(record, User {})
	if (err != nil) { fmt.Println("Error", err) }
	fmt.Println(user)

	records, err := col.ReadAll()
	if (err != nil) { fmt.Println("Error", err) }
	fmt.Println()
	allUsers, err := database.ParseMany(records, User {})
	if (err != nil) { fmt.Println("Error", err) }
	fmt.Println(allUsers)

	// if err := col.Delete("John"); err != nil { fmt.Println("Error", err) }
}