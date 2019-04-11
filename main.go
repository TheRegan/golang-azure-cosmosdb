package main

import (
	"fmt"
	"log"
	"time"
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	database string
	username string
	password string
)

func init() {
	database = "your-database-name"
	username = "your-user-name"
	password = "your-password"
}

//Player is an example of a document we can add to the collection
type Player struct {
	FirstName     string
	LastName      string
	Bats          string
	Throws        string
	Position      string
	Weight        int
	HeightFeet    int
	HeightInches  int
}

func main() {
	//Create a context to use with the connection
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	//Create the URI we will use to connect to out cosmosDB
	connecturi := fmt.Sprintf(
		"mongodb://%s:%s@%s.documents.azure.com:10255/?ssl=true",
		username,
		password,
		database)

	//Connect to the DB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connecturi))

	//Check for any errors
	if err != nil {
		log.Fatal(err)
	}

	//Ping the DB to confirm the connection
	err = client.Ping(ctx, nil)

	//Check for any errors
	if err != nil {
		log.Fatal(err)
	}

	//Print confirmation of connection
	fmt.Println("Connected to MongoDB!")

	//Select a collection to work with. Collections are synonymous with Azure cosmos contatiners.
	//See https://docs.microsoft.com/en-us/azure/cosmos-db/databases-containers-items for more info
	collection := client.Database(database).Collection("player")

	//Change the timeout of our context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)

	//
	//Inserting documents
	//

	//Create an example player
	jackie := Player{
		FirstName:     "Jackie",
		LastName:      "Robinson",
		Bats:          "Right",
		Throws:        "Right",
		Position:      "2B",
		Weight:        200,
		HeightFeet:    5,
		HeightInches:  11}

	//Insert the example player into our cosmosDB
	res, err := collection.InsertOne(ctx, jackie)
	fmt.Println("Inserted a single document from a struct: ", res.InsertedID)

	//We can also add documents inline:
	res, err = collection.InsertOne(ctx, bson.M{
		"firstname":    "Joe",
		"lastname":     "Carter",
		"bats":         "Right",
		"throws":       "Right",
		"position":     "OF",
		"weight":       215,
		"heightfeet":   6,
		"heightinches": 3})

	fmt.Println("Inserted a single document by constructing BSON: ", res.InsertedID)

	//Or insert many records:
	albert := Player{
		FirstName:     "Albert",
		LastName:      "Pujols",
		Bats:          "Right",
		Throws:        "Right",
		Position:      "1B",
		Weight:        235,
		HeightFeet:    6,
		HeightInches:  3}

	babe := Player{
		FirstName:     "Babe",
		LastName:      "Ruth",
		Bats:          "Left",
		Throws:        "Left",
		Position:      "OF",
		Weight:        215,
		HeightFeet:    6,
		HeightInches:  2}

	players := []interface{}{albert, babe}

	manyres, err := collection.InsertMany(ctx, players)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted multiple documents: ", manyres.InsertedIDs)

	//
	//Retrieving documents
	//

	//Retrieve a single document from our cosmosDB
	var result Player

	err = collection.FindOne(ctx, bson.D{{"lastname", "Robinson"}}).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(
		"Found result.\nName: ", result.FirstName, result.LastName,
		"\nBats: ", result.Bats,
		"\nThrows: ", result.Throws)

	//Retrieve multiple documents from our cosmosDB
	opts := options.Find()
	opts.SetLimit(2)

	//create a filter - thus will find all players of postion "OF"
	fil := bson.D{{"position", "OF"}}

	//get a cursor which we can use to iterate through the results
	cur, err := collection.Find(ctx, fil, opts)
	if err != nil {
		log.Fatal(err)
	}

	i := 1

	fmt.Println("\nFound multiple results:")

	//iterate over results and print them to the screen
	for cur.Next(ctx) {
		//create a variable that we can decode our document into
		var p Player
		err := cur.Decode(&p)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(
			"Result ", i,
			":\nName: ", p.FirstName, p.LastName,
			"\nBats: ", p.Bats,
			"\nThrows: ", p.Throws)
		i++
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	//Close the cursor when we're Finished
	cur.Close(ctx)

	//
	//Update documents
	//

	f := bson.D{{"position", "2B"}}

	u := bson.D{
		{"$set", bson.D{
			{"position", "1B"},
		}},
	}

	ur, err := collection.UpdateOne(ctx, f, u)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Updated", ur.ModifiedCount, "players")

	//
	//Delete document
	//

	//Delete one
	_, err = collection.DeleteOne(ctx, bson.D{{"lastname", "Pujols"}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Deleted a single document")

	//Delete many
	del, err := collection.DeleteMany(ctx, bson.D{{"bats", "Right"}})
	if err !=nil {
		log.Fatal(err)
	}
	fmt.Println("Deleted ", del.DeletedCount, "documents using delete many")

	//Close the connection
	err= client.Disconnect(ctx)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Connection closed")
	}
}
