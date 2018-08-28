package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

var recType = []string{"USER"}

///////////////////////////////////////////////////////////////////
// User object to store the personal information of user
// Note: Fields Should start with Upper case to reflect in payload.
///////////////////////////////////////////////////////////////////
type Users struct {
	UserId    string
	AssetType string //Type = USER
	EmailId   string
	Password  string
	FirstName string
	LastName  string
	Mobile    string
}

type SimpleAsset struct {
}

////////////////////////////////////////////////////////////
// Chain Code kick-off main function
////////////////////////////////////////////////////////////
func main() {
	err := shim.Start(new(SimpleAsset))
	if err != nil {
		fmt.Println("Error Starting Item Asset Handler chaincode: %s", err)
	}
}

///////////////////////////////////////////////////////////////////////////
// Init is called during chaincode instantiation to initialize any data.
///////////////////////////////////////////////////////////////////////////
func (t *SimpleAsset) Init(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("Init() Initialization Complete ")
	return shim.Success(nil)
}

/////////////////////////////////////////////////////////////
//Invoke is called per transaction on the chaincode
/////////////////////////////////////////////////////////////
func (t *SimpleAsset) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	fmt.Println("Invoke() : function is : ", function)
	fmt.Println("Invoke() : args is : ", args)
	if function[0:1] == "i" {
		return t.Insert(stub, function, args)
	}
	if function[0:1] == "q" {
		return t.Query(stub, function, args)
	}
        if function[0:1] == "d" {
		return t.Delete(stub, function, args)
	}
	return shim.Error("Invoke() : Invalid function name - function names begin with either i or q or d")
}

///////////////////////////////////////////////////////////////////////
// Executed when transaction is of type insert or update
///////////////////////////////////////////////////////////////////////
func (t *SimpleAsset) Insert(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	if CheckRecType(args[1]) == true {
		InsertRequest := InsertFunction(function)
		if InsertRequest != nil {
			response := InsertRequest(stub, function, args)
			return response
		}
	} else {
		errorStr := "Insert() : Invalid recType : " + args[1]
		fmt.Println(errorStr)
		return shim.Error(errorStr)

	}
	return shim.Success(nil)
}

///////////////////////////////////////////////////////////////////////
// Executed when transaction is of type Delete
///////////////////////////////////////////////////////////////////////
func (t *SimpleAsset) Delete(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	if CheckRecType(args[0]) == true {
		DeleteRequest := DeleteFunction(function)
		if DeleteRequest != nil {
			response := DeleteRequest(stub, function, args)
			return response
		}
	} else {
		errorStr := "Delete() : Invalid recType : " + args[0]
		fmt.Println(errorStr)
		return shim.Error(errorStr)

	}
	return shim.Success(nil)
}

////////////////////////////////////////////////////////////////////////////
// Executed when transaction is of type query
////////////////////////////////////////////////////////////////////////////
func (t *SimpleAsset) Query(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	var response pb.Response
	fmt.Println("Query() : Rec Type to be extracted : ", args[0])
	if len(args) < 1 {
		fmt.Println("Query() : Include at least 1 argument key")
		return shim.Error("Query() : Excepting Rec Type and key value for query")
	}
	QueryRequest := QueryFunction(function)
	if QueryRequest != nil {
		response = QueryRequest(stub, function, args)
	} else {
		errorStr := "Query() : Invalid function call : " + function
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	if response.Status != shim.OK {
		errorStr := "Query() : Object not found"
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	return response
}

//////////////////////////////////////////////////////////////////////////////////////
// Higher order function which returns appropriate insert function to use
//////////////////////////////////////////////////////////////////////////////////////
func InsertFunction(fname string) func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	InsertFunc := map[string]func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response{
		"iPostUsers": PostUsers,
	}
	return InsertFunc[fname]
}

//////////////////////////////////////////////////////////////////////////////////////
// Higher order function which returns appropriate query function to use
//////////////////////////////////////////////////////////////////////////////////////
func QueryFunction(fname string) func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	QueryFunc := map[string]func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response{
		"qGetUsers": GetUsers,
	}
	return QueryFunc[fname]
}

//////////////////////////////////////////////////////////////////////////////////////
// Higher order function which returns appropriate query function to use
//////////////////////////////////////////////////////////////////////////////////////
func DeleteFunction(fname string) func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	DeleteFunc := map[string]func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response{
		"dDelUsers": DelUsers,
	}
	return DeleteFunc[fname]
}
////////////////////////////////////////////////////////////////
// Update Object - Create or Replace the object with given data
////////////////////////////////////////////////////////////////
func UpdateObject(stub shim.ChaincodeStubInterface, objectType string, keys []string, objectData []byte) error {
	compositeKey, _ := stub.CreateCompositeKey(objectType, keys)
	fmt.Println("compositeKey is %s", compositeKey)
	fmt.Println("Object Type is %s", objectType)
	fmt.Println("keys is ", keys)
	// Add Object JSON to world state
	err := stub.PutState(compositeKey, objectData)
	if err != nil {
		fmt.Println("UpdateObject() : Error inserting object into world state database %s", err)
		return err
	}
	return nil
}

///////////////////////////////////////////////////////
// Puts User Object as an Asset in blockchain
///////////////////////////////////////////////////////
func PostUsers(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	fmt.Println("PostUsers() Args are ", args)
	record, err := CreateUsers(args)
	if err != nil {
		return shim.Error(err.Error())
	}
	buff, err := UsersToJSON(record)
	if err != nil {
		errorStr := "PostUsers() : Failed to create Users object for : " + args[0]
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	keys := []string{args[0]}
	err = UpdateObject(stub, "USER", keys, buff)
	if err != nil {
		fmt.Println("PostUsers() : write error while inserting record")
		return shim.Error("PostUsers() : write error while inserting record : Error - " + err.Error())
	}
	return shim.Success(buff)
}

////////////////////////////////////////////////////////
// Creates Users Object from arguments
////////////////////////////////////////////////////////
func CreateUsers(args []string) (Users, error) {
	var err error
	var aUser Users
	fmt.Println("Inside CreateUsers, Args are: ", args)
	_, err = strconv.Atoi(args[0])
	if err != nil {
		return aUser, errors.New("CreateUsers(): User ID should be an integer")
	}
	aUser = Users{args[0], args[1], args[2], args[3], args[4], args[5], args[6]}
	fmt.Println("CreateUsers(): Users : ", aUser)
	return aUser, nil
}

//////////////////////////////////////////////////////////
// Converts an User Object to a JSON String
//////////////////////////////////////////////////////////
func UsersToJSON(user Users) ([]byte, error) {
	ajson, err := json.Marshal(user)
	if err != nil {
		fmt.Println("UsersToJSON error: ", err)
		return nil, err
	}
	fmt.Println("UsersToJSON created: ", ajson)
	return ajson, nil
}

//////////////////////////////////////////////////////////
// Converts JSON String to Users Object
//////////////////////////////////////////////////////////
func JSONToUsers(user []byte) (Users, error) {
	ur := Users{}
	err := json.Unmarshal(user, &ur)
	if err != nil {
		fmt.Println("JSONToUsers error: ", err)
		return ur, err
	}
	fmt.Println("JSONToUsers created: ", ur)
	return ur, err
}

/////////////////////////////////////////////////////////////////
// Query Object by type and key
/////////////////////////////////////////////////////////////////
func QueryObject(stub shim.ChaincodeStubInterface, objectType string, keys []string) ([]byte, error) {
	compoundKey, _ := stub.CreateCompositeKey(objectType, keys)
	fmt.Println("QueryObject() : Compound Key : ", compoundKey)

	bytes, err := stub.GetState(compoundKey)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

//////////////////////////////////////////////////////////////////
// Retrieve User Object
//////////////////////////////////////////////////////////////////
func GetUsers(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	bytes, err := QueryObject(stub, args[0], []string{args[1]})
	if err != nil {
		errorStr := "GetUsers() : Failed to query user object for id : " + args[1]
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	if err != nil {
		errorStr := "GetUsers() : Failed to query complete user object for id : " + args[1]
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	fmt.Println("GetUsers() : Response : Successful")
	return shim.Success(bytes)
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Retrieve the object based on the key and simply delete it
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////
func DeleteObject(stub shim.ChaincodeStubInterface, objectType string, keys []string) error {

	// Convert keys to  compound key
	compositeKey, _ := stub.CreateCompositeKey(objectType, keys)

	// Remove object from the State Database
	err := stub.DelState(compositeKey)
	if err != nil {
		fmt.Println("DeleteObject() : Error deleting Object into State Database %s", err)
		return err
	}
	fmt.Println("DeleteObject() : ", "Object : ", objectType, " Key : ", compositeKey)

	return nil
}
//////////////////////////////////////////////////////////////////
// Retrieve User Object
//////////////////////////////////////////////////////////////////
func DelUsers(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	err := DeleteObject(stub, args[0], []string{args[1]})
	if err != nil {
		errorStr := "DelUsers() : Failed to Delete user object for id : " + args[1]
		fmt.Println(errorStr)
		return shim.Error(errorStr)
	}
	fmt.Println("GetUsers() : Response : Successful")
	return shim.Success(nil)
}
/////////////////////////////////////////////////////
// Checks whether record type is valid
/////////////////////////////////////////////////////
func CheckRecType(rt string) bool {
	for _, val := range recType {
		fmt.Println("Val is : ", val)
		if val == rt {
			return true
		}
	}
	return false
}
