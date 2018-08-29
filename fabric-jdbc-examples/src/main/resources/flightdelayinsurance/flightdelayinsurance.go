package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var recType = []string{"USER", "USRPOLICY", "POLICY", "FLIGHT", "FLIGHTPOLICY"}

var Objects = []string{"PARTY", "CASHTXN", "User", "Auction", "AucInit", "AucOpen", "Bid", "Trans", "Order"}

type User struct {
	Email     string
	RecType   string //USER
	FirstName string
	LastName  string
	Address   string
	DOB       string
	Password  string
}

type PolicyInform struct {
	PolicyID     string
	RecType      string //USRPOLICY
	UserID       string
	PolicyTypeID string
	FlightID     string
	Source       string
	Destn        string
	STD          string
	DOT          string
	ClaimStatus  string // NONE, APPROVED,COMPLETED
}
type PolicyType struct {
	ID      string
	RecType string // POLICY
	Name    string
	Details string
}

type FlightInfor struct {
	ID      string
	Rectype string // FLIGHT
	Source  string
	Destn   string
	SDT     string
	ADT     string
}

type FlightPolicyMap struct {
	FlightID string
	DOT      string //FLIGHTPOLICY
	RecType  string
	PolicyID string
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// A Map that holds ObjectNames and the number of Keys
// This information is used to dynamically Create, Update
// Replace , and Query the Ledger
// In this model all attributes in a table are strings
/////////////////////////////////////////////////////////////////////////////////////////////////////

func GetNumberOfKeys(tname string) int {
	ObjectMap := map[string]int{
		"User":            1,
		"PolicyInform":    1,
		"PolicyType":      1,
		"FlightInfor":     1,
		"FlightPolicyMap": 1,
	}
	return ObjectMap[tname]
}


////////////////////////////////////////////////////////////////////////////
// Update the Object - Replace current data with replacement
// Register users into this table
////////////////////////////////////////////////////////////////////////////
func UpdateObject(stub shim.ChaincodeStubInterface, objectType string, keys []string, objectData []byte) error {

	// Check how many keys

	err := VerifyAtLeastOneKeyIsPresent(objectType, keys)
	if err != nil {
		return err
	}

	// Convert keys to  compound key
	compositeKey, _ := stub.CreateCompositeKey(objectType, keys)

	fmt.Println("compositeKey is %s", compositeKey)
	fmt.Println("Object Type is %s", objectType)
	fmt.Println("keys is ", keys)
	// Add Object JSON to state
	err = stub.PutState(compositeKey, objectData)
	if err != nil {
		fmt.Println("UpdateObject() : Error inserting Object into State Database %s", err)
		return err
	}

	return nil

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Replaces the Entry in the Ledger
// The existing object is simply queried and the data contents is replaced with
// new content
////////////////////////////////////////////////////////////////////////////////////////////////////////////
func ReplaceObject(stub shim.ChaincodeStubInterface, objectType string, keys []string, objectData []byte) error {

	// Check how many keys

	err := VerifyAtLeastOneKeyIsPresent(objectType, keys)
	if err != nil {
		return err
	}

	// Convert keys to  compound key
	compositeKey, _ := stub.CreateCompositeKey(objectType, keys)

	// Add Party JSON to state
	err = stub.PutState(compositeKey, objectData)
	if err != nil {
		fmt.Println("ReplaceObject() : Error replacing Object in State Database %s", err)
		return err
	}

	fmt.Println("ReplaceObject() : - end init object ", objectType)
	return nil
}

////////////////////////////////////////////////////////////////////////////
// Query a User Object by Object Name and Key
// This has to be a full key and should return only one unique object
////////////////////////////////////////////////////////////////////////////
func QueryObject(stub shim.ChaincodeStubInterface, objectType string, keys []string) ([]byte, error) {

	// Check how many keys
	err := VerifyAtLeastOneKeyIsPresent(objectType, keys)
	if err != nil {
		return nil, err
	}
	compoundKey, _ := stub.CreateCompositeKey(objectType, keys)
	fmt.Println("QueryObject() : Compound Key : ", compoundKey)
	//compoundKey := "User1001"

	Avalbytes, err := stub.GetState(compoundKey)
	if err != nil {
		return nil, err
	}
	return Avalbytes, nil
}

////////////////////////////////////////////////////////////////////////////
// Query a User Object by Object Name and Key
// This has to be a full key and should return only one unique object
////////////////////////////////////////////////////////////////////////////
func QueryObjectWithProcessingFunction(stub shim.ChaincodeStubInterface, objectType string, keys []string, fname func(shim.ChaincodeStubInterface, []byte, []string) error) ([]byte, error) {

	// Check how many keys

	err := VerifyAtLeastOneKeyIsPresent(objectType, keys)
	if err != nil {
		return nil, err
	}

	compoundKey, _ := stub.CreateCompositeKey(objectType, keys)
	fmt.Println("QueryObject: Compound Key : ", compoundKey)

	Avalbytes, err := stub.GetState(compoundKey)
	if err != nil {
		return nil, err
	}

	if Avalbytes == nil {
		return nil, fmt.Errorf("QueryObject: No Data Found for Compound Key : ", compoundKey)
	}

	// Perform Any additional processing of data
	fmt.Println("fname() : Successful - Proceeding to fname")

	err = fname(stub, Avalbytes, keys)
	if err != nil {
		fmt.Println("QueryLedger() : Cannot execute  : ", fname)
		jsonResp := "{\"fname() Error\":\" Cannot create Object for key " + compoundKey + "\"}"
		return Avalbytes, errors.New(jsonResp)
	}

	return Avalbytes, nil
}

////////////////////////////////////////////////////////////////////////////
// Get a List of Rows based on query criteria from the OBC
// The getList Function
////////////////////////////////////////////////////////////////////////////
func GetKeyList(stub shim.ChaincodeStubInterface, args []string) (shim.StateQueryIteratorInterface, error) {

	// Define partial key to query within objects namespace (objectType)
	objectType := args[0]

	// Check how many keys

	err := VerifyAtLeastOneKeyIsPresent(objectType, args[1:])
	if err != nil {
		return nil, err
	}

	// Execute the Query
	// This will execute a key range query on all keys starting with the compound key
	resultsIterator, err := stub.GetStateByPartialCompositeKey(objectType, args[1:])
	if err != nil {
		return nil, err
	}

	defer resultsIterator.Close()

	// Iterate through result set
	var i int
	for i = 0; resultsIterator.HasNext(); i++ {

		// Retrieve the Key and Object
		myCompositeKey, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}
		fmt.Println("GetList() : my Value : ", myCompositeKey)
	}
	return resultsIterator, nil
}


////////////////////////////////////////////////////////////////////////////
// This function verifies if the number of key provided is at least 1 and
// < the the max keys defined for the Object
////////////////////////////////////////////////////////////////////////////

func VerifyAtLeastOneKeyIsPresent(objectType string, args []string) error {

	// Check how many keys
	nKeys := GetNumberOfKeys(objectType)
	nCol := len(args)
	if nCol == 1 {
		return nil
	}

	if nCol < 1 {
		error_str := fmt.Sprintf("VerifyAtLeastOneKeyIsPresent() Failed: Atleast 1 Key must is needed :  nKeys : %s, nCol : %s ", nKeys, nCol)
		fmt.Println(error_str)
		return errors.New(error_str)
	}

	return nil
}

func InvokeFunction(fname string) func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	InvokeFunc := map[string]func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response{
		"iCreatePolicy":    CreatePolicy,
		"iCreateUser":      CreateUser,
		"iSelectPolicy":    SelectPolicy,
		"iCreateFlightRec": CreateFlightRec,
		"iUpdateATD":       UpdateATD,
	}
	return InvokeFunc[fname]
}

func QueryFunction(fname string) func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {
	QueryFunc := map[string]func(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response{
		"qGetQuery": GetQuery,
	}
	return QueryFunc[fname]
}

type SimpleChaincode struct {
}

////////////////////////////////////////////////////////////////////////////////
// Chain Code Kick-off Main function
////////////////////////////////////////////////////////////////////////////////
func main() {

	// maximize CPU usage for maximum performance
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("Starting Flight Delay Insurance APP ")

	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Println("Error starting Flight Delay Insurance Application chaincode: %s", err)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SimpleChaincode - Init Chaincode implementation - The following sequence of transactions can be used to test the Chaincode
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("[Flight Delay Insurance Aplication] Init")
	fmt.Println("Init() Initialization Complete ")
	return shim.Success(nil)
}

func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {

	function, args := stub.GetFunctionAndParameters()

	fmt.Println("ARGS IS ", args)
	fmt.Println("==========================================================")
	fmt.Println("BEGIN Function ====> ", function)
	if function[0:1] == "i" {
		fmt.Println("==========================================================")
		return t.invoke(stub, function, args)
	}

	if function[0:1] == "q" {
		fmt.Println("==========================================================")
		return t.query(stub, function, args)
	}

	fmt.Println("==========================================================")

	return shim.Error("Invoke: Invalid Function Name - function names begin with a q or i")

}

func (t *SimpleChaincode) invoke(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Check Type of Transaction and apply business rules
	// before adding record to the block chain
	// In this version, the assumption is that args[1] specifies recType for all defined structs
	// Newer structs - the recType can be positioned anywhere and ChkReqType will check for recType
	// example:
	// ./peer chaincode invoke -l golang -n mycc -c '{"Function": "PostBid", "Args":["1111", "BID", "1", "1000", "300", "1200"]}'
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	InvokeRequest := InvokeFunction(function)
	if InvokeRequest != nil {
		response := InvokeRequest(stub, function, args)
		return (response)
	}
	return shim.Success(nil)
}

func (t *SimpleChaincode) query(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	// var buff []byte
	var response pb.Response
	fmt.Println("Query() : ID Extracted and Type = ", args[0])
	fmt.Println("Query() : Args supplied : ", args)

	if len(args) < 1 {
		fmt.Println("Query() : Include at least 1 arguments Key ")
		return shim.Error("Query() : Expecting Transation type and Key value for query")
	}

	QueryRequest := QueryFunction(function)
	if QueryRequest != nil {
		response = QueryRequest(stub, function, args)
	} else {
		fmt.Println("Query() Invalid function call : ", function)
		response_str := "Query() : Invalid function call : " + function
		return shim.Error(response_str)
	}

	if response.Status != shim.OK {
		fmt.Println("Query() Object not found : ", args[0])
		response_str := "Query() : Object not found : " + args[0]
		return shim.Error(response_str)
	}
	return response
}
func GetQuery(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	var err error

	fmt.Println("GetQuery() : Args are  ", args)

	keys := []string{args[1]}
	// Get the Object and Display it
	Avalbytes, err := QueryObject(stub, args[0], keys)
	if err != nil {
		fmt.Println("GetQuery() : Failed to Query Object ")
		jsonResp := "{\"Error\":\"Failed to get  Object Data for " + args[0] + "\"}"
		return shim.Error(jsonResp)
	}

	if Avalbytes == nil {
		fmt.Println("GetQuery() : Incomplete Query Object ")
		jsonResp := "{\"Error\":\"Incomplete information about the key for " + args[0] + "\"}"
		return shim.Error(jsonResp)
	}

	fmt.Println("GetQuery() : Response : Successfull -")
	return shim.Success(Avalbytes)
}

// Policy Type OBJECT CREATION
func CreatePolicy(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	fmt.Println(" Inside CreatePolicy() Args are ", args)
	record, err := CreatePolicyObj(args[0:]) //
	if err != nil {
		return shim.Error(err.Error())
	}
	buff, err := CPtoJSON(record) //

	if err != nil {
		error_str := "CreatePolicy() : Failed Cannot create object buffer for write : " + args[1]
		fmt.Println(error_str)
		return shim.Error(error_str)
	} else {
		// Update the ledger with the Buffer Data
		// err = stub.PutState(args[0], buff)
		keys := []string{args[0]}
		err = UpdateObject(stub, "POLICY", keys, buff)
		if err != nil {
			fmt.Println("CreatePolicy() : write error while inserting record")
			return shim.Error("CreatePolicy() : write error while inserting record : Error - " + err.Error())
		}
	}

	return shim.Success(buff)
}
func CreatePolicyObj(args []string) (PolicyType, error) {

	var err error
	var pType PolicyType

	// Validate Swift Code is an integer

	fmt.Println("Inside CreatePolicyObj  Args are ", args)
	_, err = strconv.Atoi(args[0])
	if err != nil {
		return pType, errors.New("CreatePolicyObj() : Swift ID should be an integer")
	}

	pType = PolicyType{args[0], args[1], args[2], args[3]}
	fmt.Println("CreatePolicyObj() : Policy Type Object : ", pType)

	return pType, nil
}

//////////////////////////////////////////////////////////
// Converts an Policy Object to a JSON String
//////////////////////////////////////////////////////////
func CPtoJSON(pType PolicyType) ([]byte, error) {

	ajson, err := json.Marshal(pType)
	if err != nil {
		fmt.Println("CPtoJSON error: ", err)
		return nil, err
	}
	fmt.Println("CPtoJSON created: ", ajson)
	return ajson, nil
}

//////////////////////////////////////////////////////////
// Converts a JSON String to Policy Object
//////////////////////////////////////////////////////////
func JSONtoCP(pType []byte) (PolicyType, error) {

	ur := PolicyType{}
	err := json.Unmarshal(pType, &ur)
	if err != nil {
		fmt.Println("JSONtoCP error: ", err)
		return ur, err
	}
	fmt.Println("JSONtoCP created: ", ur)
	return ur, err
}

// USER OBJECT CREATION

func CreateUser(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	fmt.Println(" Inside CreateUser() Args are ", args)
	record, err := CreateUserObj(args[0:]) //
	if err != nil {
		return shim.Error(err.Error())
	}
	buff, err := UsertoJSON(record) //

	if err != nil {
		error_str := "CreateUser() : Failed Cannot create object buffer for write : " + args[1]
		fmt.Println(error_str)
		return shim.Error(error_str)
	} else {
		keys := []string{args[0]}
		err = UpdateObject(stub, "USER", keys, buff)
		if err != nil {
			fmt.Println("CreateUser() : write error while inserting record")
			return shim.Error("CreateUser() : write error while inserting record : Error - " + err.Error())
		}
	}
	return shim.Success(buff)
}
func CreateUserObj(args []string) (User, error) {

	var userObj User

	fmt.Println("Inside CreatePartyObj  Args are ", args)

	userObj = User{args[0], args[1], args[2], args[3], args[4], args[5], args[6]}
	fmt.Println("CreateUserObj() : User Object : ", userObj)

	return userObj, nil
}

//////////////////////////////////////////////////////////
// Converts an User Object to a JSON String
//////////////////////////////////////////////////////////
func UsertoJSON(userObj User) ([]byte, error) {

	ajson, err := json.Marshal(userObj)
	if err != nil {
		fmt.Println("UsertoJSON error: ", err)
		return nil, err
	}
	fmt.Println("UsertoJSON created: ", ajson)
	return ajson, nil
}

//////////////////////////////////////////////////////////
// Converts a JSON String to User Object
//////////////////////////////////////////////////////////
func JSONtoUser(userObj []byte) (User, error) {

	ur := User{}
	err := json.Unmarshal(userObj, &ur)
	if err != nil {
		fmt.Println("JSONtoUser error: ", err)
		return ur, err
	}
	fmt.Println("JSONtoUser created: ", ur)
	return ur, err
}

// PolicyInform OBJECT CREATION

func SelectPolicy(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	fmt.Println(" Inside SelectPolicy() Args are ", args)
	record, err := SelectPolicyObj(args[0:]) //
	if err != nil {
		return shim.Error(err.Error())
	}
	buff, err := PolicyObjtoJSON(record) //

	if err != nil {
		error_str := "SelectPolicy() : Failed Cannot create object buffer for write : " + args[1]
		fmt.Println(error_str)
		return shim.Error(error_str)
	} else {
		keys := []string{args[0]}
		err = UpdateObject(stub, "USRPOLICY", keys, buff)
		if err != nil {
			fmt.Println("SelectPolicy() : write error while inserting record")
			return shim.Error("SelectPolicy() : write error while inserting record : Error - " + err.Error())
		}
	}

	//First get the Existing record of this FlightID & DOT
	//If it's there then update the PolicyID column with the new one
	//Otherwise Create a new record with FlightID & DOT

	flightId := args[4] + args[8]
	response := GetPloicyIDs(stub, flightId)
	if response.Status != shim.OK {
		var flightMapObj FlightPolicyMap
		flightMapObj = FlightPolicyMap{args[4], args[8], "FLIGHTPOLICY", args[0]}
		buffObj, _ := FlightPolicyMapObjtoJSON(flightMapObj)
		keys := []string{args[4] + args[8]}
		err = UpdateObject(stub, "FLIGHTPOLICY", keys, buffObj)
		if err != nil {
			fmt.Println("SelectPolicy() : write error while inserting record for map between FlightID & Date of Travel ")
			return shim.Error("SelectPolicy() : write error while inserting record for map between FlightID & Date of Travel : Error - " + err.Error())
		}
	} else {
		flghtPolicyDet := response.Payload
		flightPolicyrec, _ := JSONtoFlightPolicyMapObj(flghtPolicyDet)
		flightPolicyrec.PolicyID += "," + args[0]
		buff, err := FlightPolicyMapObjtoJSON(flightPolicyrec) //
		if err != nil {
			error_str := "SelectPolicy() : Failed Cannot create object buffer for write : " + args[1]
			fmt.Println(error_str)
			return shim.Error(error_str)
		} else {
			keys := []string{args[4] + args[8]}
			err = UpdateObject(stub, "FLIGHTPOLICY", keys, buff)
			if err != nil {
				fmt.Println("SelectPolicy() : write error while inserting record")
				return shim.Error("SelectPolicy() : write error while inserting record : Error - " + err.Error())
			}
		}
	}
	return shim.Success(buff)
}
func SelectPolicyObj(args []string) (PolicyInform, error) {

	var polObj PolicyInform

	fmt.Println("Inside SelectPolicyObj  Args are ", args)

	polObj = PolicyInform{args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9]}
	fmt.Println("SelectPolicyObj() : Select Policy Object : ", polObj)

	return polObj, nil
}

//////////////////////////////////////////////////////////
// Converts an Policy Object to a JSON String
//////////////////////////////////////////////////////////
func PolicyObjtoJSON(polObj PolicyInform) ([]byte, error) {

	ajson, err := json.Marshal(polObj)
	if err != nil {
		fmt.Println("PolicyObjtoJSON error: ", err)
		return nil, err
	}
	fmt.Println("PolicyObjtoJSON created: ", ajson)
	return ajson, nil
}

//////////////////////////////////////////////////////////
// Converts a JSON String to Policy Object
//////////////////////////////////////////////////////////
func JSONtoPolicyObj(polObj []byte) (PolicyInform, error) {

	ur := PolicyInform{}
	err := json.Unmarshal(polObj, &ur)
	if err != nil {
		fmt.Println("JSONtoPolicyObj error: ", err)
		return ur, err
	}
	fmt.Println("JSONtoPolicyObj created: ", ur)
	return ur, err
}

//////////////////////////////////////////////////////////
// Converts an FlightPolicy Object to a JSON String
//////////////////////////////////////////////////////////
func FlightPolicyMapObjtoJSON(polObj FlightPolicyMap) ([]byte, error) {

	ajson, err := json.Marshal(polObj)
	if err != nil {
		fmt.Println("FlightPolicyMapObjtoJSON error: ", err)
		return nil, err
	}
	fmt.Println("FlightPolicyMapObjtoJSON created: ", ajson)
	return ajson, nil
}

// Flight Information OBJECT CREATION

func CreateFlightRec(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	fmt.Println(" Inside CreateFlightRec() Args are ", args)
	record, err := CreateFlightRecObj(args[0:]) //
	if err != nil {
		return shim.Error(err.Error())
	}
	buff, err := FlightObjtoJSON(record) //

	if err != nil {
		error_str := "CreateFlightRec() : Failed Cannot create object buffer for write : " + args[1]
		fmt.Println(error_str)
		return shim.Error(error_str)
	} else {
		// Key Should be FlightID & Date of Flying
		dtime := strings.Split(args[4], "T")
		dof := dtime[0]
		keyId := args[0] + dof
		keys := []string{keyId}
		err = UpdateObject(stub, "FLIGHT", keys, buff)
		if err != nil {
			fmt.Println("CreateFlightRec() : write error while inserting record")
			return shim.Error("CreateFlightRec() : write error while inserting record : Error - " + err.Error())
		}
	}
	return shim.Success(buff)
}
func CreateFlightRecObj(args []string) (FlightInfor, error) {

	var fltObj FlightInfor

	fmt.Println("Inside CreateFlightRecObj  Args are ", args)

	fltObj = FlightInfor{args[0], args[1], args[2], args[3], args[4], args[5]}
	fmt.Println("CreateFlightRecObj() : Flight Object Creation : ", fltObj)

	return fltObj, nil
}

//////////////////////////////////////////////////////////
// Converts an FlightRecord Object to a JSON String
//////////////////////////////////////////////////////////
func FlightObjtoJSON(fltObj FlightInfor) ([]byte, error) {

	ajson, err := json.Marshal(fltObj)
	if err != nil {
		fmt.Println("FlightObjtoJSON error: ", err)
		return nil, err
	}
	fmt.Println("FlightObjtoJSON created: ", ajson)
	return ajson, nil
}

//////////////////////////////////////////////////////////
// Converts a JSON String to FlightRecord Object
//////////////////////////////////////////////////////////
func JSONtoFlightObj(fltObj []byte) (FlightInfor, error) {

	ur := FlightInfor{}
	err := json.Unmarshal(fltObj, &ur)
	if err != nil {
		fmt.Println("JSONtoFlightObj error: ", err)
		return ur, err
	}
	fmt.Println("JSONtoFlightObj created: ", ur)
	return ur, err
}

// Updating the Flight Actual Departure time and check the delay between expected and actual and based on the
// result update the claim status to approved otherwise completed

func UpdateATD(stub shim.ChaincodeStubInterface, function string, args []string) pb.Response {

	fmt.Println(" Inside CreateFlightRec() Args are ", args)
	// Validate Item record
	response := ValidateFlightRec(stub, args[0]+args[1])
	if response.Status != shim.OK {
		fmt.Println("UpdateATD() : Failed Could not Validate Flight ID Object in Blockchain ", args[0])
		return shim.Error("UpdateATD() : Failed Could not Validate Flight ID Object in Blockchain")
	}

	flightDet := response.Payload

	flghtRec, err := JSONtoFlightObj(flightDet)
	if err != nil {
		fmt.Println("UpdateATD() : Cannot UnMarshall Flight Record")
		return shim.Error("UpdateATD(): Cannot UnMarshall Flight record: " + args[0])
	}

	//Update the Actual departure Time
	flghtRec.ADT = args[1] + "T" + args[2]

	buff, err := FlightObjtoJSON(flghtRec) //
	if err != nil {
		error_str := "UpdateATD() : Failed Cannot create object buffer for write : " + args[1]
		fmt.Println(error_str)
		return shim.Error(error_str)
	} else {
		keyId := flghtRec.ID+args[1]
		keys := []string{keyId}
		err = UpdateObject(stub, "FLIGHT", keys, buff)
		if err != nil {
			fmt.Println("UpdateATD() : write error while inserting record")
			return shim.Error("UpdateATD() : write error while inserting record : Error - " + err.Error())
		}
	}

	// Convert the SDT & ADT into time
	etdtime := ConvertStrToTime(flghtRec.SDT)
	atdtime := ConvertStrToTime(flghtRec.ADT)

	diff := atdtime.Sub(etdtime)
	hrs := int(diff.Hours())

	// For Now it's Hardcoded for Greater than 2 Hours we need to publish the
	// Claim status to approved
	var claimStatus string
	claimStatus = "NONE"
	if hrs >= 2 {
		fmt.Println("It Means we need to Process the Claims")
		claimStatus = "APPROVED"
	}

	flightId := args[0] + args[1]
	response = GetPloicyIDs(stub, flightId)
	if response.Status != shim.OK {
		fmt.Println("UpdateATD() : Failed Could not Validate Flight ID Object in Blockchain ", args[0])
		return shim.Error("UpdateATD() : Failed Could not Validate Flight ID Object in Blockchain")
	}

	// First Find all the Policy Holder with this Flight No
	// & Update the Policy Holder Status Based on the Actual
	// Departure time
	flghtPolicyDet := response.Payload
	flightPolicyrec, _ := JSONtoFlightPolicyMapObj(flghtPolicyDet)
	policyIds := strings.Split(flightPolicyrec.PolicyID, ",")
	fmt.Println(policyIds)
	for _, policyId := range policyIds {
		fmt.Println(policyId)
		UpdatePolicyDetails(stub, policyId, claimStatus)
	}

	return shim.Success([]byte(""))
}

func ValidateFlightRec(stub shim.ChaincodeStubInterface, flghtId string) pb.Response {

	// Get the Item Objects and Display it
	args := []string{flghtId}
	Avalbytes, err := QueryObject(stub, "FLIGHT", args)
	if err != nil {
		fmt.Println("ValidateFlightRec() : Failed - Cannot find valid Order record for OrderID ", flghtId)
		jsonResp := "{\"Error\":\"Failed to get Owner Object Data for " + flghtId + "\"}"
		return shim.Error(jsonResp)
	}

	if Avalbytes == nil {
		fmt.Println("ValidateFlightRec() : Failed - Incomplete owner record for ART  ", flghtId)
		jsonResp := "{\"Error\":\"Failed - Incomplete information about the owner for " + flghtId + "\"}"
		return shim.Error(jsonResp)
	}

	return shim.Success(Avalbytes)
}
func GetPloicyIDs(stub shim.ChaincodeStubInterface, policyIDS string) pb.Response {

	// Get the Item Objects and Display it
	args := []string{policyIDS}
	Avalbytes, err := QueryObject(stub, "FLIGHTPOLICY", args)
	if err != nil {
		fmt.Println("GetPloicyIDs() : Failed - Cannot find valid Order record for FlightID & DOT ", policyIDS)
		jsonResp := "{\"Error\":\"Failed - Cannot find valid Order record for FlightID & DOT " + policyIDS + "\"}"
		return shim.Error(jsonResp)
	}

	if Avalbytes == nil {
		fmt.Println("GetPloicyIDs() : Failed - Incomplete owner record for ART  ", policyIDS)
		jsonResp := "{\"Error\":\"Failed - Incomplete information about the owner for " + policyIDS + "\"}"
		return shim.Error(jsonResp)
	}

	return shim.Success(Avalbytes)
}
func GetPloicyID(stub shim.ChaincodeStubInterface, policyID string) pb.Response {

	// Get the Item Objects and Display it
	args := []string{policyID}
	Avalbytes, err := QueryObject(stub, "USRPOLICY", args)
	if err != nil {
		fmt.Println("GetPloicyID() : Failed - Cannot find valid Order record for FlightID & DOT ", policyID)
		jsonResp := "{\"Error\":\"Failed - Cannot find valid Order record for FlightID & DOT " + policyID + "\"}"
		return shim.Error(jsonResp)
	}

	if Avalbytes == nil {
		fmt.Println("GetPloicyID() : Failed - Incomplete owner record for ART  ", policyID)
		jsonResp := "{\"Error\":\"Failed - Incomplete information about the owner for " + policyID + "\"}"
		return shim.Error(jsonResp)
	}
	return shim.Success(Avalbytes)
}
func UpdatePolicyDetails(stub shim.ChaincodeStubInterface, policyID string, status string) {
	response := GetPloicyID(stub, policyID)
	if response.Status != shim.OK {
		fmt.Println("UpdatePolicyDetails() : Failed Could not Validate Flight ID Object in Blockchain ", policyID)
		//return shim.Error("UpdatePolicyDetails() : Failed Could not Validate Flight ID Object in Blockchain")
	}

	policyDet := response.Payload

	policyRec, err := JSONtoPolicyObj(policyDet)
	if err != nil {
		fmt.Println("UpdatePolicyDetails() : Cannot UnMarshall Flight Record")
		//return shim.Error("UpdatePolicyDetails(): Cannot UnMarshall Flight record: " + policyID)
	}

	//Update the Actual departure Time
	policyRec.ClaimStatus = status
	buff, err := PolicyObjtoJSON(policyRec) //
	if err != nil {
		error_str := "UpdatePolicyDetails() : Failed Cannot create object buffer for write : " + policyID
		fmt.Println(error_str)
		//return shim.Error(error_str)
	} else {
		keys := []string{policyRec.PolicyID}
		err = UpdateObject(stub, "USRPOLICY", keys, buff)
		if err != nil {
			fmt.Println("UpdatePolicyDetails() : write error while inserting record")
			//return shim.Error("UpdatePolicyDetails() : write error while inserting record : Error - " + err.Error())
		}
	}
}

//////////////////////////////////////////////////////////
// Converts a JSON String to FlightPolicyMap Object
//////////////////////////////////////////////////////////
func JSONtoFlightPolicyMapObj(polObj []byte) (FlightPolicyMap, error) {

	ur := FlightPolicyMap{}
	err := json.Unmarshal(polObj, &ur)
	if err != nil {
		fmt.Println("JSONtoFlightPolicyMapObj error: ", err)
		return ur, err
	}
	fmt.Println("JSONtoFlightPolicyMapObj created: ", ur)
	return ur, err
}

func ConvertStrToTime(str string) time.Time {

	fmt.Println(str)
	dtime := strings.Split(str, "T")
	fmt.Println(dtime[0])
	fmt.Println(dtime[1])
	date := strings.Split(dtime[0], "-")
	yr, _ := strconv.Atoi(date[0])
	mm, _ := strconv.Atoi(date[1])
	dd, _ := strconv.Atoi(date[2])

	timestmp := strings.Split(dtime[1], ":")
	hh, _ := strconv.Atoi(timestmp[0])
	mi, _ := strconv.Atoi(timestmp[1])
	ss, _ := strconv.Atoi(timestmp[2])

	return time.Date(yr, time.Month(mm), dd, hh, mi, ss, 0, time.UTC)
}
