package workerpkg

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

func doWork(client *http.Client, id int) {
	time.Sleep(6 * time.Second)
	resp, err := client.Get("https://3e9162a6-aa82-4ee0-b25b-f8dc686c70cf.mock.pstmn.io")
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func doWorkString(client *http.Client, id int) string {
	// time.Sleep(6 * time.Second)

	//resp, err := client.Get("https://3e9162a6-aa82-4ee0-b25b-f8dc686c70cf.mock.pstmn.io")
	resp, err := client.Get("https://bing.com")
	if err != nil {
		return fmt.Sprintf("Worker %d: request failed\n", id)
	}
	defer resp.Body.Close()

	return fmt.Sprintf("Worker %d: received status code %d\n", id, resp.StatusCode)
}

func doWorklog(client *http.Client, id int) {
	time.Sleep(6 * time.Second)

	resp, err := client.Get("https://3e9162a6-aa82-4ee0-b25b-f8dc686c70cf.mock.pstmn.io")
	if err != nil {
		fmt.Printf("Goroutine %d: request failed\n", id)
		return
	}

	defer resp.Body.Close()
	fmt.Printf("Goroutine %d: received status code %d\n", id, resp.StatusCode)
}

func worker(id int, client *http.Client, jobs <-chan int, wg *sync.WaitGroup) {

	for jobID := range jobs {
		doWork(client, jobID)
		wg.Done()
	}
}

func workerChan(id int, client *http.Client, jobs <-chan int, results chan<- string, wg *sync.WaitGroup) {

	for jobID := range jobs {
		results <- doWorkString(client, jobID)
		wg.Done()
	}

}

func Concurrent(count int) {
	client := http.Client{Timeout: 2 * time.Second}
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(id int) {
			defer wg.Done()
			doWork(&client, id)
		}(i)
	}

	wg.Wait()
}

func ConcurrentWithChan(count int) {
	client := http.Client{Timeout: 2 * time.Second}
	results := make(chan string, count)
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(id int) {
			defer wg.Done()
			results <- doWorkString(&client, id)
		}(i)
	}

	wg.Wait()

	close(results)
	// Now logging phase (AFTER all jobs done)
	//res := 0
	//for _ = range results {
	//	res++
	//}
	//
	//fmt.Printf("ConcurrentWorker has dispatched %d jobs\n", count)
}

func ConcurrentWorkerPool(totalJobs, workerCount int) {

	client := &http.Client{Timeout: 2 * time.Second}
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(totalJobs)

	for w := 0; w < workerCount; w++ {
		go worker(w, client, jobs, &wg)
	}

	for i := 0; i < totalJobs; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all jobs to be finished
	wg.Wait()
}

func ConcurrentWorkerPoolWithChan(totalJobs, workerCount int) {

	client := &http.Client{Timeout: 2 * time.Second}
	jobs := make(chan int)
	results := make(chan string, totalJobs)
	var wg sync.WaitGroup
	wg.Add(totalJobs)

	// Start workers
	for w := 0; w < workerCount; w++ {
		go workerChan(w, client, jobs, results, &wg)
	}

	// dispath asynchronously
	for i := 0; i < totalJobs; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()

	// All work done, close results channel
	close(results)

	// Now logging phase (AFTER all jobs done)
	//count := 0
	//for _ = range results {
	//	count++
	//}
	//
	//fmt.Printf("ConcurrentWorkerPoolChan has dispatched %d jobs\n", count)
}

func ConcurrentWorkerDispatchers(totalJobs, workerCount int, dispatchers int) {
	client := &http.Client{Timeout: 2 * time.Second}
	jobs := make(chan int, totalJobs) // buffered
	results := make(chan string, totalJobs)
	var jobWg sync.WaitGroup
	jobWg.Add(totalJobs)
	// Start workers
	for w := 0; w < workerCount; w++ {
		go workerChan(w, client, jobs, results, &jobWg)
	}

	if dispatchers <= 0 {
		dispatchers = 1
	}
	if dispatchers > totalJobs {
		dispatchers = totalJobs
	}
	jobsPerDispatcher := totalJobs / dispatchers
	var dispatchWg sync.WaitGroup
	dispatchWg.Add(dispatchers)

	// Start multiple dispatchers
	for d := 0; d < dispatchers; d++ {
		start := d * jobsPerDispatcher
		end := start + jobsPerDispatcher

		// Ensure the last dispatcher handles any remainder jobs
		if d == dispatchers-1 {
			end = totalJobs
		}

		// Safeguard: ensure start and end are valid
		if start >= totalJobs {
			// This dispatcher has nothing to do
			dispatchWg.Done()
			continue
		}
		if end > totalJobs {
			end = totalJobs
		}

		// Only create a goroutine if there are actual jobs to dispatch
		if end > start {
			go func(start, end int) {
				defer dispatchWg.Done()
				for i := start; i < end; i++ {
					jobs <- i
				}
			}(start, end)
		} else {
			dispatchWg.Done()
		}
	}

	// Wait for dispatchers and close jobs
	go func() {
		dispatchWg.Wait()
		close(jobs)
	}()

	// Wait for workers and close results
	go func() {
		jobWg.Wait()
		close(results)
	}()

	//Read results
	count := 0
	for _ = range results {
		count++
	}
	//
	//fmt.Printf("ConcurrentWorkerDispatchers has dispatched %d jobs\n", count)
}
