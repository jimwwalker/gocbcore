package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
)

// CollectionManifestCollection represents a collection entry within a manifest.
type CollectionManifestCollection struct {
	UID  uint64
	Name string
}

// UnmarshalJSON is used to build a CollectionManifestCollection entry from bytes.
func (item *CollectionManifestCollection) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	item.UID = decUID
	item.Name = decData.Name
	return nil
}

// CollectionManifestScope represents a scope entry within a manifest.
type CollectionManifestScope struct {
	UID         uint64
	Name        string
	Collections []CollectionManifestCollection
}

// UnmarshalJSON is used to build a CollectionManifestScope entry from bytes.
func (item *CollectionManifestScope) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID         string                         `json:"uid"`
		Name        string                         `json:"name"`
		Collections []CollectionManifestCollection `json:"collections"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	item.UID = decUID
	item.Name = decData.Name
	item.Collections = decData.Collections
	return nil
}

// CollectionManifest represents a manifest from the server, it contains details about available scopes
// and collections.
type CollectionManifest struct {
	UID    uint64
	Scopes []CollectionManifestScope
}

// UnmarshalJSON is used to build a CollectionManifest entry from bytes.
func (item *CollectionManifest) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID    string                    `json:"uid"`
		Scopes []CollectionManifestScope `json:"scopes"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	item.UID = decUID
	item.Scopes = decData.Scopes
	return nil
}

// CollectionManifestCallback is invoked upon completion of a GetCollectionManifest operation.
type CollectionManifestCallback func(manifest []byte, err error)

// GetCollectionManifest fetches the latest manifest from the server.
func (agent *Agent) GetCollectionManifest(cb CollectionManifestCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		log.Printf("Got Manifest %+v %+v %+v", err, resp, req)
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resp.Value, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetManifest,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback: handler,
	}
	log.Printf("Requesting manifest %+v", req)
	return agent.dispatchOp(req)
}

// CollectionIdCallback is invoked upon completion of a GetCollectionID operation.
type CollectionIdCallback func(manifestID uint64, collectionID uint32, err error)

// GetCollectionID fetches the collection id and manifest id that the collection belongs to, given a scope name
// and collection name.
func (agent *Agent) GetCollectionID(scopeName string, collectionName string, cb CollectionIdCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		log.Printf("Got Collection ID %+v %+v %+v", err, resp, req)
		if err != nil {
			cb(0, 0, err)
			return
		}

		manifestID := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])
		cb(manifestID, collectionID, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetID,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte(fmt.Sprintf("%s.%s", scopeName, collectionName)),
			Value:    nil,
			Vbucket:  0,
		},
		Callback: handler,
	}
	log.Printf("Requesting collection ID %+v", req)
	return agent.dispatchOp(req)
}
