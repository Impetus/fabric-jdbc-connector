// assettransfer.go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

const (
	ASSET       = "ASSET"
	PARTICIPANT = "PART"
)

type Asset struct {
	Id          int         `"json: id"`
	Name        string      `"json: name"`
	Participant Participant `"json: participant"`
}

type Participant struct {
	Id   int    `"json: id"`
	Name string `"json: name"`
}

type AssetTransfer struct {
}

func main() {
	err := shim.Start(new(AssetTransfer))
	if err != nil {
		fmt.Println("Error starting Asset Transfer chaincode %s", err)
	}
}

func (at *AssetTransfer) Init(stub shim.ChaincodeStubInterface) pb.Response {
	participants := []Participant{
		{2000, "Ronnie"},
		{2001, "Sergio"},
		{2002, "Tony"},
	}
	assets := []Asset{
		{1000, "Laptop", participants[0]},
		{1001, "Desktop", participants[1]},
		{1002, "Laptop", participants[2]},
		{1003, "Server", participants[2]},
		{1004, "Server", participants[2]},
	}
	for _, participant := range participants {
		at.StoreParticipant(stub, participant)
	}
	for _, asset := range assets {
		at.StoreAsset(stub, asset)
	}
	fmt.Println("Init() Initialization Complete ")
	return shim.Success(nil)
}

func (at *AssetTransfer) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	if function == "transferAsset" {
		return at.TransferAsset(stub, args)
	}
	if function == "getAsset" {
		return at.GetAsset(stub, args)
	}
	return shim.Error("Invoke: Invalid function name " + function)
}

func (at *AssetTransfer) TransferAsset(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 3 {
		return shim.Error("TransferAsset(): Invalid number of arguments. Should be 3")
	}
	assetId, err := strconv.Atoi(args[0])
	if err != nil {
		return shim.Error("TransferAsset(): Asset id may not be integer: " + err.Error())
	}
	fromParticipantId, err := strconv.Atoi(args[1])
	if err != nil {
		return shim.Error("TransferAsset(): from Participant id may not be integer: " + err.Error())
	}
	toParticipantId, err := strconv.Atoi(args[2])
	if err != nil {
		return shim.Error("TransferAsset(): to Participant id may not be integer: " + err.Error())
	}
	asset, err := at.QueryAsset(stub, assetId)
	if err != nil {
		return shim.Error(err.Error())
	}
	if asset.Participant.Id != fromParticipantId {
		return shim.Error("TransferAsset(): Asset doesn't belong to the participant specified: " + err.Error())
	}
	participant, err := at.QueryParticipant(stub, toParticipantId)
	if err != nil {
		return shim.Error(err.Error())
	}
	asset.Participant = participant
	return at.StoreAsset(stub, asset)
}

func (at *AssetTransfer) GetAsset(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 1 {
		return shim.Error("GetAsset(): Invalid number of arguments. Should be 1")
	}
	assetId, err := strconv.Atoi(args[0])
	if err != nil {
		return shim.Error("GetAsset(): Asset id may not be integer")
	}
	asset, err := at.QueryAsset(stub, assetId)
	if err != nil {
		return shim.Error(err.Error())
	}
	ajson, err := AssetToJson(asset)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(ajson)
}

func (at *AssetTransfer) StoreAsset(stub shim.ChaincodeStubInterface, asset Asset) pb.Response {
	ajson, err := AssetToJson(asset)
	if err != nil {
		fmt.Println("StoreAsset() error: ", err)
		return shim.Error(err.Error())
	}
	compositeKey, _ := stub.CreateCompositeKey(ASSET, []string{strconv.Itoa(asset.Id)})
	err = stub.PutState(compositeKey, ajson)
	if err != nil {
		return shim.Error("StoreAsset(): Error storing Asset: " + err.Error())
	}
	return shim.Success(ajson)
}

func (at *AssetTransfer) QueryAsset(stub shim.ChaincodeStubInterface, id int) (asset Asset, err error) {
	compositeKey, _ := stub.CreateCompositeKey(ASSET, []string{strconv.Itoa(id)})
	bytes, err := stub.GetState(compositeKey)
	if err != nil {
		return
	}
	asset, err = JsonToAsset(bytes)
	return
}

func (at *AssetTransfer) QueryParticipant(stub shim.ChaincodeStubInterface, id int) (participant Participant, err error) {
	compositeKey, _ := stub.CreateCompositeKey(PARTICIPANT, []string{strconv.Itoa(id)})
	bytes, err := stub.GetState(compositeKey)
	if err != nil {
		return
	}
	participant, err = JsonToParticipant(bytes)
	return
}

func (at *AssetTransfer) StoreParticipant(stub shim.ChaincodeStubInterface, participant Participant) pb.Response {
	ajson, err := ParticipantToJson(participant)
	if err != nil {
		fmt.Println("StoreParticipant() error: ", err)
		return shim.Error(err.Error())
	}
	compositeKey, _ := stub.CreateCompositeKey(PARTICIPANT, []string{strconv.Itoa(participant.Id)})
	err = stub.PutState(compositeKey, ajson)
	if err != nil {
		return shim.Error("StoreParticipant(): Error storing Asset: " + err.Error())
	}
	return shim.Success(ajson)
}

func ParticipantToJson(participant Participant) ([]byte, error) {
	ajson, err := json.Marshal(participant)
	if err != nil {
		fmt.Println("ParticipantToJson() error: ", err)
		return nil, err
	}
	fmt.Println("ParticipantToJson() JSON created")
	return ajson, nil
}

func JsonToParticipant(ajson []byte) (Participant, error) {
	participant := Participant{}
	err := json.Unmarshal(ajson, &participant)
	if err != nil {
		fmt.Println("JsonToParticipant() error: ", err)
		return participant, err
	}
	fmt.Println("JsonToParticipant() Participant created")
	return participant, nil
}

func AssetToJson(asset Asset) ([]byte, error) {
	ajson, err := json.Marshal(asset)
	if err != nil {
		fmt.Println("AssetToJson() error: ", err)
		return nil, err
	}
	fmt.Println("AssetToJson() JSON created")
	return ajson, nil
}

func JsonToAsset(ajson []byte) (Asset, error) {
	asset := Asset{}
	err := json.Unmarshal(ajson, &asset)
	if err != nil {
		fmt.Println("JsonToAsset() error: ", err)
		return asset, err
	}
	fmt.Println("JsonToAsset() Asset created")
	return asset, nil
}
