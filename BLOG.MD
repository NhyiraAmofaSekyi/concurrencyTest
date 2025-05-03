

DO WORK 

```go
package main
import (
	"fmt"
	"net/http"
	"time"
)

func waitOneSecond(client *http.Client, id int) {
	time.Sleep(1 * time.Second)

	resp, err := client.Get("https://3e9162a6-aa82-4ee0-b25b-f8dc686c70cf.mock.pstmn.io")
	if err != nil {
		fmt.Printf("Goroutine %d: request failed\n", id)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("Goroutine %d: received status code %d\n", id, resp.StatusCode)
}
```




Work 
an http request to a remote server 
 

Approach 1
run aggregate the requests and run each request as a goroutine 
each utilising the same http client

if each go routine calls a function that logs we could run into an error where we run out of file discreptors
error with this approach
```bash
panic: too many concurrent operations on a single file or socket (max 1048575)

goroutine 3498429 [running]:
internal/poll.(*fdMutex).rwlock(0x140001000c0, 0xd0?)
        /usr/local/go/src/internal/poll/fd_mutex.go:147 +0x128
internal/poll.(*FD).writeLock(...)
        /usr/local/go/src/internal/poll/fd_mutex.go:239
internal/poll.(*FD).Write(0x140001000c0, {0x143855efa80, 0x24, 0x40})
        /usr/local/go/src/internal/poll/fd_unix.go:368 +0x48
os.(*File).write(...)
        /usr/local/go/src/os/file_posix.go:46
os.(*File).Write(0x1400010e028, {0x143855efa80?, 0x24, 0x141bc3fef50?})
        /usr/local/go/src/os/file.go:195 +0x58
fmt.Fprintf({0x100be9ac8, 0x1400010e028}, {0x100af8f95, 0x1f}, {0x141bc3fef50, 0x1, 0x1})
        /usr/local/go/src/fmt/print.go:225 +0x88
fmt.Printf(...)
        /usr/local/go/src/fmt/print.go:233
main.waitOneSecond(0x14000116de0, 0x16a8eb)
        /Users/nhyiraamofa-sekyi/Documents/projects/ConcurrencyTest/main.go:15 +0x98
main.concurrent.func1(0x0?)
        /Users/nhyiraamofa-sekyi/Documents/projects/ConcurrencyTest/main.go:30 +0x54
created by main.concurrent in goroutine 1
        /Users/nhyiraamofa-sekyi/Documents/projects/ConcurrencyTest/main.go:28 +0x84
```

Approach 2 uses a routine so each func instead of logging or returning
puts the result of its operation into a channel to be consumed by a set number of loggers ( below max file discriptors)

the other approach utilises a pool of workers to process jobs 
as they come into a job channel the instead of having a goroutine for every job we have a set number of goroutines reading from a job channel
```go
package workerpkg

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)
func doWorkString(client *http.Client, id int) string {
	time.Sleep(6 * time.Second)

	resp, err := client.Get("https://3e9162a6-aa82-4ee0-b25b-f8dc686c70cf.mock.pstmn.io")
	if err != nil {
		return fmt.Sprintf("Worker %d: request failed\n", id)
	}
	defer resp.Body.Close()

	return fmt.Sprintf("Worker %d: received status code %d\n", id, resp.StatusCode)
}


func workerChan(id int, client *http.Client, jobs <-chan int, results chan<- string, wg *sync.WaitGroup) {

	for jobID := range jobs {
		results <- doWorkString(client, jobID)
		wg.Done()
	}

}
```


Operations: 100000 Runs: 3  Cores: 12 Workers: 960000

========================================== BENCHMARK RESULTS ==========================================
Function                       Time            Memory (MiB)    Sys Mem (MiB)   GC Runs    Goroutines
--------------------------------------------------------------------------------------------------------
Concurrent                     36.839855597s   606             724             3          219477    
ConcurrentWithChan             35.585087861s   575             174             2          221555    
ConcurrentWorkerPool           25.748918903s   722             2132            1          960001    
ConcurrentWorkerPoolWithChan   18.599647167s   694             40              0          960001    
ConcurrentWorkerPoolispatcher  597.658167ms    160             0               0          955284    
======================================================================================================

All tests completed. Total time: 9m42.178670667s

Final memory statistics:
Alloc = 739 MiB TotalAlloc = 9185 MiB   Sys = 9558 MiB  NumGC = 33

error 
```bash 
Operations:1000000 Runs:3  Cores:12 Workers: 960000  
runtime: failed to create new OS thread
```

