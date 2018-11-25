package aerosync

import (
	"errors"
	"math"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	ase "github.com/aerospike/aerospike-client-go/types"
)

const (
	DefaultLockTimeout   = 5 * time.Second
	UnlimitedLockTimeout = math.MaxUint32 * time.Second
	DefaultRetryCount    = 0
	DefaultRetryDelay    = 100 * time.Millisecond
)

var (
	// ErrFailed is returned when mutex cannot be locked
	ErrFailed = errors.New("failed to lock mutex")
)

// Options describe the options for the lock
type Options struct {
	// The maximum duration to lock a key for
	// Default: 5s
	LockTimeout time.Duration

	// The number of time the acquisition of a lock will be retried.
	// Default: 0 = do not retry
	RetryCount int

	// RetryDelay is the amount of time to wait between retries.
	// Default: 100ms
	RetryDelay time.Duration
}

var DefaultOptions = Options{
	LockTimeout: DefaultLockTimeout,
	RetryCount:  DefaultRetryCount,
	RetryDelay:  DefaultRetryDelay,
}

type Mutex struct {
	client *as.Client
	key    *as.Key
	opts   Options
}

func NewMutex(client *as.Client, key *as.Key, opts *Options) *Mutex {
	if opts == nil {
		opts = &DefaultOptions
	}

	return &Mutex{client: client, key: key, opts: *opts}
}

func (m *Mutex) Lock() error {
	retryDelay := time.NewTimer(m.opts.RetryDelay)
	defer retryDelay.Stop()

	for i := 0; i < (1 + m.opts.RetryCount); i++ {
		policy := as.NewWritePolicy(0, uint32(m.opts.LockTimeout.Seconds()))
		policy.RecordExistsAction = as.CREATE_ONLY
		policy.MaxRetries = 0 //m.opts.RetryCount
		//policy.SleepBetweenRetries = m.opts.RetryDelay

		bin := as.NewBin("", []byte(""))

		if err := m.client.PutBins(policy, m.key, bin); err != nil {
			aErr, ok := err.(ase.AerospikeError)
			if !ok {
				return err // unknown error
			}
			if aErr.ResultCode() != ase.KEY_EXISTS_ERROR {
				return aErr
			}
			// if KEY_EXISTS_ERROR, continue for loop
		} else {
			return nil // locked
		}

		// if not the last attempt, reset delay
		if i != m.opts.RetryCount {
			retryDelay.Reset(m.opts.RetryDelay)
			select {
			case <-retryDelay.C:
			}
		}
	}
	return ErrFailed
}

func (m *Mutex) Unlock() error {
	policy := as.NewWritePolicy(0, 0)
	if _, err := m.client.Delete(policy, m.key); err != nil {
		return err
	}

	return nil
}
