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
	name     string
	arg      func()
	interval time.Duration
	timeout  time.Duration
	debug    bool

	timer *time.Timer

	mongoClient *mongoose.MongooseClient
}

func NewJobLockTask(mongoClient *mongoose.MongooseClient, name string, arg func(), interval time.Duration, timeout time.Duration, debug bool) (*JobLockTask, error) {

	_, err := mongoClient.RegisterModel(&JobLock{})
	if err != nil {
		return nil, err
	}

	return &JobLockTask{
		name:        name,
		arg:         arg,
		interval:    interval,
		timeout:     timeout,
		debug:       debug,
		mongoClient: mongoClient,
	}, nil
}

func (t *JobLockTask) Run(ctx context.Context) error {

	if t.mongoClient == nil {
		return errors.New("mongoClient is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if t.interval == 0 {
		return errors.New("`Time Interval is missing!`")
	}

	if t.arg == nil {
		return errors.New("`What this timer should to run?`")
	}

	t.updateTimer()

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Job %s terminated!", t.name)
			default:

				<-t.timer.C

				//lock
				lock, err := t.isNotLockThenLock(ctx)
				if err != nil {
					log.Printf("Job %s failed to lock with error: %s\n", t.name, err.Error())
				}
				if lock {
					if t.debug {
						log.Printf("Running job %s \n", t.name)
					}

					// run the task
					t.arg()
					if t.debug {
						log.Printf("Finished job %s \n", t.name)
					}

					t.UnLock(ctx)
				}
				t.updateTimer()
			}
		}
	}()

	return nil
}

func (t *JobLockTask) RunOnce(ctx context.Context) error {

	if t.mongoClient == nil {
		return errors.New("mongoClient is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if t.arg == nil {
		return errors.New("`What this timer should to run?`")
	}

	go func() {
		//lock
		lock, err := t.isNotLockThenLock(ctx)
		if err != nil {
			log.Printf("Job %s failed to lock with error: %s\n", t.name, err.Error())
		}
		if lock {
			if t.debug {
				log.Printf("Running job %s \n", t.name)
			}

			// run the task
			t.arg()
			if t.debug {
				log.Printf("Finished job %s \n", t.name)
			}

			t.UnLock(ctx)
		}
	}()

	return nil
}

func (t *JobLockTask) isNotLockThenLock(ctx context.Context) (bool, error) {

	ExistingLock := JobLock{}

	lockModel, err := t.mongoClient.GetModel("JobLock")
	if err != nil {
		return false, err
	}

	lockModel.FindOne(&ExistingLock, bson.M{"name": t.name})

	if !ExistingLock.ID.IsZero() {

		if ExistingLock.Expiry.Before(time.Now()) {
			t.UnLock(ctx)
		} else {
			return false, errors.New("already locked by another process")
		}
	}

	NewLock := JobLock{
		ID:     primitive.NewObjectID(),
		Name:   t.name,
		Expiry: time.Now().Add(t.timeout),
	}

	if err := lockModel.Create(&NewLock); err != nil {
		return false, err
	}

	return true, nil
}

func (t *JobLockTask) UnLock(ctx context.Context) error {

	lockModel, err := t.mongoClient.GetModel("JobLock")
	if err != nil {
		return err
	}

	if err := lockModel.Delete(bson.M{"name": t.name}); err != nil {
		return err
	}

	return nil
}

func (t *JobLockTask) updateTimer() {
	next := time.Now().Add(t.interval)

	diff := next.Sub(time.Now())
	if t.timer == nil {
		t.timer = time.NewTimer(diff)
	} else {
		t.timer.Reset(diff)
	}
}
