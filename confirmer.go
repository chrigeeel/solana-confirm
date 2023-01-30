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
	rpcClient *rpc.Client
	delay     time.Duration
	tasks     map[solana.Signature]*Task
	mu        sync.RWMutex

	subCh   chan Task
	unsubCh chan solana.Signature
}

type confirmerOption func(*Confirmer)

const (
	defaultDelay = time.Second
	rpcLimit     = 256
)

func New(rpcEndpoint string, options ...confirmerOption) *Confirmer {
	c := &Confirmer{
		delay:     defaultDelay,
		rpcClient: rpc.New(rpcEndpoint),
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

func (c *Confirmer) Start() {
	go func() {
		c.run()
		time.Sleep(c.delay)
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

func (c *Confirmer) WaitForConfirmationStatus(signature solana.Signature, status rpc.ConfirmationStatusType, maxAttempts int) error {
	return <-c.Subscribe(signature, status, maxAttempts)
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
