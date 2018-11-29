package donation

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

func UpdateDonation(coll *mongo.Collection, event *model.Event) *model.Document {
	donateUpdate := &donationUpdate{}

	err := json.Unmarshal(event.Data, donateUpdate)
	if err != nil {
		err = errors.Wrap(err, "Update: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if len(donateUpdate.Filter) == 0 {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if len(donateUpdate.Update) == 0 {
		err = errors.New("blank update provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if donateUpdate.Update["itemID"] == (uuuid.UUID{}).String() {
		err = errors.New("found blank itemID in update")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	updateStats, err := coll.UpdateMany(donateUpdate.Filter, donateUpdate.Update)
	if err != nil {
		err = errors.Wrap(err, "Update: Error in UpdateMany")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	result := &updateResult{
		MatchedCount:  updateStats.MatchedCount,
		ModifiedCount: updateStats.ModifiedCount,
	}
	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Update: Error marshalling Inventory Update-result")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.Document{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        resultMarshal,
		EventAction:   event.EventAction,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
