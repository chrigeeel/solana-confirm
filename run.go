package solanaconfirm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func (c *Confirmer) run() {
	c.mu.RLock()
	var signatures []solana.Signature
	for s := range c.tasks {
		signatures = append(signatures, s)
	}
	c.mu.RUnlock()

	log.Println(len(signatures))

	if len(signatures) == 0 {
		return
	}

	chunkSignatures := chunkBy(signatures, rpcLimit)

	var results []*rpc.SignatureStatusesResult
	var wgMutex sync.Mutex
	var wg sync.WaitGroup
	for _, chunk := range chunkSignatures {
		wg.Add(1)
		go func(wg *sync.WaitGroup, chunk []solana.Signature) {
			resp, err := c.rpcClient.GetSignatureStatuses(
				context.TODO(),
				true,
				chunk...,
			)
			if err != nil {
				return
			}

			wgMutex.Lock()
			defer wgMutex.Unlock()
			results = append(results, resp.Value...)
		}(&wg, chunk)
	}
	wg.Wait()

	if len(results) != len(signatures) {
		log.Print("len results does not match len signatures, please check your rpc")
		return
	}

	for i, result := range results {
		signature := signatures[i]
		c.mu.RLock()
		task := c.tasks[signature]
		task.attempts++
		c.mu.RUnlock()

		if result == nil {
			if task.attempts >= task.maxAttempts {
				task.C <- errors.New("maximum attempts reached")
				c.Unsubscribe(task.signature)
			}
			continue
		}

		if result.Err != nil {
			task.C <- fmt.Errorf("transaction error: %v", result.Err)
			c.Unsubscribe(task.signature)
		} else if result.ConfirmationStatus == task.status {
			task.C <- nil
			c.Unsubscribe(task.signature)
		}
	}
}
