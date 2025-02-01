package joblock

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/mrhid6/go-mongoose/mongoose"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type JobLock struct {
	ID     primitive.ObjectID `json:"_id" bson:"_id"`
	Name   string             `json:"name" bson:"name"`
	Expiry time.Time          `json:"expiry" bson:"expiry"`
}

type JobLockTask struct {
	Name     string
	Arg      func()
	Interval time.Duration
	Timeout  time.Duration
	Debug    bool

	timer *time.Timer
}

func (t *JobLockTask) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if t.Interval == 0 {
		return errors.New("`Time Interval is missing!`")
	}

	if t.Arg == nil {
		return errors.New("`What this timer should to run?`")
	}

	t.updateTimer()

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Job %s terminated!", t.Name)
			default:

				<-t.timer.C

				//lock
				lock, err := t.isNotLockThenLock(ctx)
				if err != nil {
					log.Printf("Job %s failed to lock with error: %s\n", t.Name, err.Error())
				}
				if lock {
					if t.Debug {
						log.Printf("Running job %s \n", t.Name)
					}

					// run the task
					t.Arg()
					if t.Debug {
						log.Printf("Finished job %s \n", t.Name)
					}

					t.UnLock(ctx)
				}
				t.updateTimer()
			}
		}
	}()

	return nil
}

func (t *JobLockTask) isNotLockThenLock(ctx context.Context) (bool, error) {

	ExistingLock := JobLock{}

	mongoose.FindOne(bson.M{"name": t.Name}, &ExistingLock)

	if !ExistingLock.ID.IsZero() {

		if ExistingLock.Expiry.Before(time.Now()) {
			t.UnLock(ctx)
		} else {
			return false, errors.New("already locked by another process")
		}
	}

	NewLock := JobLock{
		ID:     primitive.NewObjectID(),
		Name:   t.Name,
		Expiry: time.Now().Add(t.Timeout),
	}

	if _, err := mongoose.InsertOne(&NewLock); err != nil {
		return false, err
	}

	return true, nil
}

func (t *JobLockTask) UnLock(ctx context.Context) error {

	if _, err := mongoose.DeleteOne(bson.M{"name": t.Name}, &JobLock{}); err != nil {
		return err
	}

	return nil
}

func (t *JobLockTask) updateTimer() {
	next := time.Now()
	if !next.After(time.Now()) {
		next = next.Add(t.Interval)
	}
	diff := next.Sub(time.Now())
	if t.timer == nil {
		t.timer = time.NewTimer(diff)
	} else {
		t.timer.Reset(diff)
	}
}
