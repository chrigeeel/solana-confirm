package solanaconfirm

import (
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type Task struct {
	signature   solana.Signature
	status      rpc.ConfirmationStatusType
	C           chan error
	attempts    int
	maxAttempts int
}

type Confirmer struct {
	rpcClient                *rpc.Client
	delay                    time.Duration
	searchTransactionHistory bool
	tasks                    map[solana.Signature]*Task
	mu                       sync.RWMutex

	subCh   chan Task
	unsubCh chan solana.Signature
}

type confirmerOption func(*Confirmer)

const (
	defaultDelay                    = time.Second
	defaultSearchTransactionHistory = true
	rpcLimit                        = 256
)

func New(rpcEndpoint string, options ...confirmerOption) *Confirmer {
	c := &Confirmer{
		rpcClient:                rpc.New(rpcEndpoint),
		delay:                    defaultDelay,
		searchTransactionHistory: defaultSearchTransactionHistory,
		tasks:                    make(map[solana.Signature]*Task),
		mu:                       sync.RWMutex{},

		subCh:   make(chan Task),
		unsubCh: make(chan solana.Signature),
	}

	for _, o := range options {
		o(c)
	}

	return c
}

func WithDelay(delay time.Duration) confirmerOption {
	return func(c *Confirmer) {
		c.delay = delay
	}
}

func WithSearchTransactionHistory(searchTransactionHistory bool) confirmerOption {
	return func(c *Confirmer) {
		c.searchTransactionHistory = searchTransactionHistory
	}
}

func (c *Confirmer) Start() {
	go func() {
		for {
			c.run()
			time.Sleep(c.delay)
		}
	}()

	go func() {
		for {
			select {
			case task := <-c.subCh:
				c.mu.Lock()
				c.tasks[task.signature] = &task
				c.mu.Unlock()
			case signature := <-c.unsubCh:
				c.mu.Lock()
				delete(c.tasks, signature)
				c.mu.Unlock()
			}
		}
	}()
}

func (c *Confirmer) waitForConfirmationStatus(signature solana.Signature, status rpc.ConfirmationStatusType, maxAttempts int) error {
	return <-c.Subscribe(signature, status, maxAttempts)
}

func (c *Confirmer) Processed(signature solana.Signature) error {
	timeout := 10 * time.Second
	maxAttempts := int(timeout / c.delay)
	return c.waitForConfirmationStatus(signature, rpc.ConfirmationStatusProcessed, maxAttempts)
}

func (c *Confirmer) Confirmed(signature solana.Signature) error {
	timeout := 30 * time.Second
	maxAttempts := int(timeout / c.delay)
	return c.waitForConfirmationStatus(signature, rpc.ConfirmationStatusConfirmed, maxAttempts)
}

func (c *Confirmer) Finalized(signature solana.Signature) error {
	timeout := 120 * time.Second
	maxAttempts := int(timeout / c.delay)
	return c.waitForConfirmationStatus(signature, rpc.ConfirmationStatusFinalized, maxAttempts)
}

func (c *Confirmer) Subscribe(signature solana.Signature, status rpc.ConfirmationStatusType, maxAttempts int) chan error {
	ch := make(chan error, 1)

	task := Task{
		signature:   signature,
		status:      status,
		maxAttempts: maxAttempts,

		C: ch,
	}

	c.subCh <- task

	return ch
}

func (c *Confirmer) Unsubscribe(signature solana.Signature) {
	c.unsubCh <- signature
}
