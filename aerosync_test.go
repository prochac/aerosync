package aerosync_test

import (
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/prochac/aerosync"
)

const (
	asHostname  = "localhost"
	asPort      = 3000
	asNamespace = "aerosync"
	asSet       = "mutex"
)

func Test_Basic(t *testing.T) {
	dr, err := newDockerResource(t)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	client, err := aerospike.NewClient(asHostname, dr.GetPort())
	if err != nil {
		t.Fatalf("Failed to start aerospike bidstore: %s", err)
	}
	defer client.Close()

	key, err := aerospike.NewKey(asNamespace, asSet, rand.Int63())
	if err != nil {
		t.Fatal("Failed to create key")
	}

	mutex := aerosync.NewMutex(client, key, nil)
	if err := mutex.Lock(); err != nil {
		t.Fatalf("Failed to lock mutex key: %s", err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatalf("Failed to unlock mutex key: %s", err)
	}
}

func Test_Concurrency(t *testing.T) {
	dr, err := newDockerResource(t)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	const workers = 100
	var global = 0

	policy := aerospike.NewClientPolicy()
	policy.ConnectionQueueSize = workers + 1

	client, err := aerospike.NewClientWithPolicyAndHost(policy, aerospike.NewHost(asHostname, dr.GetPort()))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	key, err := aerospike.NewKey(asNamespace, asSet, rand.Int63())
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := aerosync.Options{
				RetryCount:  math.MaxInt32,
				RetryDelay:  500 * time.Millisecond,
				LockTimeout: 5 * time.Second,
			}
			mutex := aerosync.NewMutex(client, key, &opts)
			_ = mutex

			if err := mutex.Lock(); err != nil {
				t.Fatalf("Failed to lock mutex key: %s", err)
			}
			defer func() {
				if err := mutex.Unlock(); err != nil {
					t.Fatalf("Failed to unlock mutex key: %s", err)
				}
			}()

			tmp := global
			tmp += 1
			global = tmp
		}(i)
	}

	wg.Wait()
	if global != workers {
		t.Fatalf("Concurrency failed")
	}
}

func Test_LockTimeout(t *testing.T) {
	dr, err := newDockerResource(t)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	const workers = 2
	var global = 0

	policy := aerospike.NewClientPolicy()
	policy.ConnectionQueueSize = workers + 1

	client, err := aerospike.NewClientWithPolicyAndHost(policy, aerospike.NewHost(asHostname, dr.GetPort()))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	key, err := aerospike.NewKey(asNamespace, asSet, rand.Int63())
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := aerosync.Options{
				RetryCount:  math.MaxInt32,
				RetryDelay:  500 * time.Millisecond,
				LockTimeout: 1 * time.Second,
			}
			mutex := aerosync.NewMutex(client, key, &opts)
			_ = mutex

			if err := mutex.Lock(); err != nil {
				t.Fatalf("Failed to lock mutex key: %s", err)
			}
			defer func() {
				if err := mutex.Unlock(); err != nil {
					t.Fatalf("Failed to unlock mutex key: %s", err)
				}
			}()

			tmp := global
			tmp += 1
			time.Sleep(5 * time.Second)
			global = tmp
		}(i)
	}

	wg.Wait()
	if global != 1 {
		t.Fatalf("Lock doesn't timeouted correctly")
	}
}
func Test_RetryExceeded(t *testing.T) {
	dr, err := newDockerResource(t)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	policy := aerospike.NewClientPolicy()
	policy.ConnectionQueueSize = 2

	client, err := aerospike.NewClientWithPolicyAndHost(policy, aerospike.NewHost(asHostname, dr.GetPort()))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	key, err := aerospike.NewKey(asNamespace, asSet, rand.Int63())
	if err != nil {
		panic(err)
	}

	go func() {
		opts := aerosync.Options{
			RetryCount:  1, //math.MaxInt32,
			RetryDelay:  500 * time.Millisecond,
			LockTimeout: 5 * time.Second,
		}
		mutex := aerosync.NewMutex(client, key, &opts)
		_ = mutex

		if err := mutex.Lock(); err != nil {
			t.Fatalf("Failed to lock mutex key: %s", err)
		}
		defer func() {
			if err := mutex.Unlock(); err != nil {
				t.Fatalf("Failed to unlock mutex key: %s", err)
			}
		}()

		time.Sleep(1 * time.Second) // hold the lock
	}()
	time.Sleep(10 * time.Millisecond) // just to be shore who will be locked first

	opts := aerosync.Options{
		RetryCount:  1, //math.MaxInt32,
		RetryDelay:  500 * time.Millisecond,
		LockTimeout: 5 * time.Second,
	}
	mutex := aerosync.NewMutex(client, key, &opts)

	if err := mutex.Lock(); err != nil {
		if err == aerosync.ErrFailed {
			return // This is ok
		}
		t.Fatalf("Failed to lock mutex key: %s", err)
	}

	t.Fatalf("Second mutex shouldn't be locked")
}

type dockerResource struct {
	t *testing.T

	pool     *dockertest.Pool
	resource *dockertest.Resource
}

func (a dockerResource) GetPort() int {
	portInt, err := strconv.ParseInt(a.resource.GetPort(strconv.Itoa(asPort)+"/tcp"), 10, 32)
	if err != nil {
		log.Panic("Failed to parse port number")
	}
	return int(portInt)
}

func (a dockerResource) Close() {
	if a.pool != nil && a.resource != nil {
		if err := a.pool.Purge(a.resource); err != nil {
			a.t.Log("Failed to purge aerospike test resources")
		}
	}
}

// starts the aerospike docker image and populates asDocker global variable
func newDockerResource(t *testing.T) (*dockerResource, error) {
	var dr dockerResource
	dr.t = t

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a docker pool")
	}
	dr.pool = pool

	resource, err := dr.pool.Run("aerospike/aerospike-server", "latest", []string{"NAMESPACE=" + asNamespace})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to start aerospike resource")
	}
	dr.resource = resource

	if err := dr.pool.Retry(func() error {
		as, err := aerospike.NewClient(asHostname, dr.GetPort())
		if err != nil {
			return err
		}
		as.Close()
		return nil
	}); err != nil {
		if purgeErr := dr.pool.Purge(dr.resource); purgeErr != nil {
			return nil, errors.Wrap(err, "Failed to start resource and also failed to purge it: "+err.Error())
		} else {
			return nil, errors.Wrap(err, "Failed to start aerospike resource")
		}
	}

	return &dr, nil
}
