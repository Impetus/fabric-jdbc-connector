/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/

// etltracker.go
package main

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

type Queries struct {
	Database  string
	Table     string
	Query     string
	timestamp time.Time
}

type EtlTracker struct {
}

func ToString(currentTime time.Time) string {
	return fmt.Sprintf("%d-%d-%d %d:%d:%d", currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), currentTime.Minute(),
		currentTime.Second())
}

func main() {
	err := shim.Start(new(EtlTracker))
	if err != nil {
		fmt.Println("Error starting Etl Tracker chaincode %s", err)
	}
}

func (et *EtlTracker) Init(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("Init() Initialization Complete ")
	return shim.Success(nil)
}

func (et *EtlTracker) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	if function == "insert" {
		return et.Insert(stub, args)
	}
	if function == "query" {
		return et.Query(stub, args)
	}
	if function == "queryHistory" {
		return et.QueryHistory(stub, args)
	}
	return shim.Error("Invoke: Invalid function name " + function)
}

func (et *EtlTracker) Insert(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	database := args[0]
	table := args[1]
	query := args[2]
	queries := Queries{database, table, query, time.Now()}
	value := QueriesToCSV(queries)
	compositeKey, _ := stub.CreateCompositeKey(database, []string{table})
	err := stub.PutState(compositeKey, value)
	if err != nil {
		return shim.Error("Insert(): Error inserting change: " + err.Error())
	}
	return shim.Success(value)
}

func (et *EtlTracker) Query(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	database := args[0]
	table := args[1]
	compositeKey, _ := stub.CreateCompositeKey(database, []string{table})
	bytes, err := stub.GetState(compositeKey)
	if err != nil {
		return shim.Error("Query(): Error query current schema: " + err.Error())
	}
	return shim.Success(bytes)
}

func (et *EtlTracker) QueryHistory(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	database := args[0]
	table := args[1]
	compositeKey, _ := stub.CreateCompositeKey(database, []string{table})
	history, err := stub.GetHistoryForKey(compositeKey)
	if err != nil {
		return shim.Error("QueryHistory(): Error quering history for the queried table: " + err.Error())
	}
	str := ""
	for history.HasNext() {
		keyChange, err := history.Next()
		if err != nil {
			return shim.Error("QueryHistory(): Error quering history for the queried table: " + err.Error())
		}
		value := keyChange.GetValue()
		str = str + string(value) + "\n"
	}
	str = str[:len(str)-1]
	history.Close()
	return shim.Success([]byte(str))
}

func QueriesToCSV(queries Queries) []byte {
	csv := queries.Database + ";" + queries.Table + ";" + queries.Query + ";" + ToString(queries.timestamp)
	return []byte(csv)
}
