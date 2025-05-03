package main

import (
	workerpkg "ConcurrencyTest/worker"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// Helper function to collect memory statistics
func printMemStats(memStats *runtime.MemStats) {
	runtime.ReadMemStats(memStats)
	fmt.Printf("Alloc = %v MiB", memStats.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", memStats.TotalAlloc/1024/1024)
	fmt.Printf("\tSys = %v MiB", memStats.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", memStats.NumGC)
}

// BenchResult stores benchmark results
type BenchResult struct {
	name          string
	duration      time.Duration
	allocMiB      uint64
	sysMiB        uint64
	gcRuns        uint32
	maxGoroutines int
}

func runBenchmark(name string, count, numWorkers int, fn func()) BenchResult {
	// Force GC before starting
	debug.FreeOSMemory()

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	// Get baseline goroutine count (including this function and any background goroutines)
	baselineGoroutines := runtime.NumGoroutine()

	// Create monitoring goroutine for peak detection
	var maxGoroutines int = baselineGoroutines
	var goroutineCountMutex sync.Mutex
	monitorDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(10 * time.Millisecond) // Sample every 10ms
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				currentCount := runtime.NumGoroutine()
				goroutineCountMutex.Lock()
				if currentCount > maxGoroutines {
					maxGoroutines = currentCount
				}
				goroutineCountMutex.Unlock()
			case <-monitorDone:
				return
			}
		}
	}()

	// Start timing and run function
	startTime := time.Now()
	fn()
	elapsed := time.Since(startTime)

	// Stop the monitoring goroutine
	close(monitorDone)

	// Collect memory statistics
	runtime.ReadMemStats(&memStatsAfter)

	// Calculate memory growth
	allocDiff := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc
	sysDiff := memStatsAfter.Sys - memStatsBefore.Sys

	// Wait a moment for any lingering goroutines to finish
	time.Sleep(100 * time.Millisecond)

	// Calculate actual goroutine peak (subtract the baseline)
	goroutineCountMutex.Lock()
	actualMaxGoroutines := maxGoroutines - baselineGoroutines
	goroutineCountMutex.Unlock()

	// Return results to be collected for table
	return BenchResult{
		name:          name,
		duration:      elapsed,
		allocMiB:      allocDiff / 1024 / 1024,
		sysMiB:        sysDiff / 1024 / 1024,
		gcRuns:        memStatsAfter.NumGC - memStatsBefore.NumGC,
		maxGoroutines: actualMaxGoroutines,
	}
}

func runBenchmarkAvg(name string, count, numWorkers, numRuns int, fn func()) BenchResult {
	if numRuns <= 0 {
		numRuns = 1 // Ensure at least one run
	}

	var totalDuration time.Duration
	var totalAllocMiB uint64
	var totalSysMiB uint64
	var totalGCRuns uint32
	var maxGoroutinesAcrossRuns int

	// Run the benchmark multiple times
	for run := 0; run < numRuns; run++ {
		// Force GC before starting
		debug.FreeOSMemory()

		var memStatsBefore, memStatsAfter runtime.MemStats
		runtime.ReadMemStats(&memStatsBefore)

		// Get baseline goroutine count (including this function and any background goroutines)
		baselineGoroutines := runtime.NumGoroutine()

		// Create monitoring goroutine for peak detection
		var maxGoroutines int = baselineGoroutines
		var goroutineCountMutex sync.Mutex
		monitorDone := make(chan struct{})

		go func() {
			ticker := time.NewTicker(10 * time.Millisecond) // Sample every 10ms
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					currentCount := runtime.NumGoroutine()
					goroutineCountMutex.Lock()
					if currentCount > maxGoroutines {
						maxGoroutines = currentCount
					}
					goroutineCountMutex.Unlock()
				case <-monitorDone:
					return
				}
			}
		}()

		// Start timing and run function
		//fmt.Printf("Running %s: run %d of %d\n", name, run+1, numRuns)
		startTime := time.Now()
		fn()
		elapsed := time.Since(startTime)

		// Stop the monitoring goroutine
		close(monitorDone)

		// Collect memory statistics
		runtime.ReadMemStats(&memStatsAfter)

		// Calculate memory growth
		allocDiff := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc
		sysDiff := memStatsAfter.Sys - memStatsBefore.Sys

		// Wait a moment for any lingering goroutines to finish
		time.Sleep(100 * time.Millisecond)

		// Calculate actual goroutine peak (subtract the baseline)
		goroutineCountMutex.Lock()
		actualMaxGoroutines := maxGoroutines - baselineGoroutines
		goroutineCountMutex.Unlock()

		// Update totals
		totalDuration += elapsed
		totalAllocMiB += allocDiff / 1024 / 1024
		totalSysMiB += sysDiff / 1024 / 1024
		totalGCRuns += memStatsAfter.NumGC - memStatsBefore.NumGC

		// Track maximum goroutines across all runs
		if actualMaxGoroutines > maxGoroutinesAcrossRuns {
			maxGoroutinesAcrossRuns = actualMaxGoroutines
		}

		// Print run results
		//fmt.Printf("  Run %d completed in %v\n", run+1, elapsed)

		// Wait between runs to let the system settle
		if run < numRuns-1 {
			time.Sleep(1000 * time.Millisecond)
		}
	}

	// Calculate averages
	avgDuration := totalDuration / time.Duration(numRuns)
	avgAllocMiB := totalAllocMiB / uint64(numRuns)
	avgSysMiB := totalSysMiB / uint64(numRuns)
	avgGCRuns := float64(totalGCRuns) / float64(numRuns)

	// Return results with averages
	return BenchResult{
		name:          name,
		duration:      avgDuration,
		allocMiB:      avgAllocMiB,
		sysMiB:        avgSysMiB,
		gcRuns:        uint32(avgGCRuns + 0.5), // Round to nearest integer
		maxGoroutines: maxGoroutinesAcrossRuns, // Use max across all runs
	}
}
func main() {
	count := 1000000
	numWorkers := runtime.NumCPU() * 800
	numRuns := 3

	fmt.Printf("Operations:%d Runs:%d  Cores:%d Workers: %d  \n", count, numRuns, runtime.NumCPU(), numWorkers)
	totalStart := time.Now()

	//results := []BenchResult{
	//	runBenchmark("Concurrent", count, 0, func() {
	//		workerpkg.Concurrent(count)
	//	}),
	//
	//	runBenchmark("ConcurrentWithChan", count, 0, func() {
	//		workerpkg.ConcurrentWithChan(count)
	//	}),
	//
	//	runBenchmark("ConcurrentWorkerPool", count, numWorkers, func() {
	//		workerpkg.ConcurrentWorkerPool(count, numWorkers)
	//	}),
	//
	//	runBenchmark("ConcurrentWorkerPoolWithChan", count, numWorkers, func() {
	//		workerpkg.ConcurrentWorkerPoolWithChan(count, numWorkers)
	//	}),
	//}

	results := []BenchResult{

		runBenchmarkAvg("ConcurrentWorkerPoolDispatcher", count, numWorkers, numRuns, func() {
			workerpkg.ConcurrentWorkerDispatchers(count, numWorkers, 0)
		}),

		runBenchmarkAvg("ConcurrentWithChan", count, numWorkers, numRuns, func() {
			workerpkg.ConcurrentWithChan(count)
		}),

		runBenchmarkAvg("ConcurrentWorkerPoolWithChan", count, numWorkers, numRuns, func() {
			workerpkg.ConcurrentWorkerPoolWithChan(count, numWorkers)
		}),
	}

	//results := []BenchResult{
	//
	//	runBenchmarkAvg("dispatchers 1", count, numWorkers, numRuns, func() {
	//		workerpkg.ConcurrentWorkerDispatchers(count, numWorkers, 0)
	//	}),
	//
	//	runBenchmarkAvg("dispatchers 100", count, numWorkers, numRuns, func() {
	//		workerpkg.ConcurrentWorkerDispatchers(count, numWorkers, 10)
	//	}),
	//
	//	runBenchmarkAvg("dispatchers 1000", count, numWorkers, numRuns, func() {
	//		workerpkg.ConcurrentWorkerDispatchers(count, numWorkers, 100)
	//	}),
	//}

	totalElapsed := time.Since(totalStart)

	fmt.Println("\n========================================== BENCHMARK RESULTS ==========================================")
	fmt.Printf("%-30s %-15s %-15s %-15s %-10s %-10s\n",
		"Function", "Time", "Memory (MiB)", "Sys Mem (MiB)", "GC Runs", "Goroutines")
	fmt.Println("--------------------------------------------------------------------------------------------------------")

	for _, r := range results {
		fmt.Printf("%-30s %-15v %-15d %-15d %-10d %-10d\n",
			r.name, r.duration, r.allocMiB, r.sysMiB, r.gcRuns, r.maxGoroutines)
	}
	fmt.Println("======================================================================================================")

	fmt.Printf("\nAll tests completed. Total time: %v\n", totalElapsed)

	// Print final memory stats
	fmt.Println("\nFinal memory statistics:")
	var finalMemStats runtime.MemStats
	printMemStats(&finalMemStats)
}

// All 10 tasks completed in 1.00151475s
// All 10000 tasks completed in 1.04389175s
// All 1000000 tasks completed in 4.743420542s

//http req with timeout
//All 10000000 tasks completed in 11m33.849407167s
//All 100000 Total time 43.949181s
// All 1000000 Total time 1m41.475595584s
//All 10000000 Total time 10m59.476046667s
// All 10000000 Total time 6m0.844757667s  - no logs
//All 10000000 Total time 7m0.455227084s
//All 10000000 Total time 4m38.782287792s

//concurrent 56gb of memory
//All 24000 Total time 32.2370455s
//All 240000 Total time 1m7.782416625s

// worker pool
// All 24000 Total time 18.983049292s
// All 240000 Total time 8m21.562023958
// All 10000000 Total time 3m31.786748917s
// All 10000000 Total time 2m34.583224292s
// All 10000000 Total time 2m25.887581625s

//simple concurrent
//All 100000 Total time 44.532927625s
//All 1000000 Total time 2m19.308551875s
// All 240000 Total time 1m2.892341125s

//worker pool with log channel
// All 10000000 Total time 52m33.678630792s

// Worker pool implementation
//func workerPool(count int, numWorkers int) {
//	if numWorkers <= 0 {
//		numWorkers = runtime.NumCPU()
//	}
//
//	// Create a shared HTTP client with optimized settings
//	client := &http.Client{
//		Timeout: 2 * time.Second,
//		Transport: &http.Transport{
//			MaxIdleConnsPerHost: numWorkers,
//			MaxConnsPerHost:     numWorkers,
//			IdleConnTimeout:     90 * time.Second,
//		},
//	}
//
//	//client := &http.Client{Timeout: time.Second * 2}
//
//	// Create a channel for distributing work
//	jobs := make(chan int, numWorkers*2) // Buffer to reduce blocking
//
//	// Create counters for progress tracking
//	var (
//		completed int64
//		failed    int64
//		mutex     sync.Mutex // For safe console output
//	)
//
//	// Start progress reporting goroutine
//	done := make(chan struct{})
//	go func() {
//		ticker := time.NewTicker(5 * time.Second)
//		defer ticker.Stop()
//
//		lastCompleted := int64(0)
//		startTime := time.Now()
//
//		for {
//			select {
//			case <-ticker.C:
//				current := atomic.LoadInt64(&completed)
//				currentFailed := atomic.LoadInt64(&failed)
//
//				// Calculate completion percentage and rate
//				percentComplete := float64(current) / float64(count) * 100
//				elapsed := time.Since(startTime).Seconds()
//				rate := float64(current) / elapsed
//
//				// Calculate ETA
//				var eta time.Duration
//				if rate > 0 {
//					remainingTasks := count - int(current)
//					etaSeconds := float64(remainingTasks) / rate
//					eta = time.Duration(etaSeconds) * time.Second
//				}
//
//				// Calculate current throughput (tasks/second)
//				throughput := float64(current-lastCompleted) / 5.0
//
//				mutex.Lock()
//				fmt.Printf("\rProgress: %.2f%% (%d/%d) | Rate: %.2f req/s | Current: %.2f req/s | Failed: %d | ETA: %v",
//					percentComplete, current, count, rate, throughput, currentFailed, eta)
//				mutex.Unlock()
//
//				lastCompleted = current
//
//				// Exit if done
//				if current >= int64(count) {
//					return
//				}
//			case <-done:
//				return
//			}
//		}
//	}()
//
//	// Create worker pool
//	var wg sync.WaitGroup
//
//	// Workers process jobs from the channel
//	fmt.Printf("Starting %d workers for %d tasks\n", numWorkers, count)
//	for w := 0; w < numWorkers; w++ {
//		wg.Add(1)
//		go func(workerID int) {
//			defer wg.Done()
//
//			for id := range jobs {
//				startTime := time.Now()
//
//				// Do the work
//				waitOneSecond(client, id)
//
//				// Update counters
//				atomic.AddInt64(&completed, 1)
//
//				// Optional: Check errors and update failure counter
//				// if err != nil {
//				//     atomic.AddInt64(&failed, 1)
//				// }
//
//				// Optional debug info
//				if workerID == 0 && id%1000 == 0 {
//					mutex.Lock()
//					fmt.Printf("\nWorker %d completed task %d in %v\n", workerID, id, time.Since(startTime))
//					mutex.Unlock()
//				}
//			}
//		}(w)
//	}
//
//	// Send all the work to the jobs channel
//	startTime := time.Now()
//	for i := 0; i < count; i++ {
//		jobs <- i
//	}
//	close(jobs) // Signal workers that no more jobs are coming
//
//	// Wait for all workers to finish
//	wg.Wait()
//	close(done) // Signal progress tracker to stop
//
//	// Print final statistics
//	elapsed := time.Since(startTime)
//	fmt.Printf("workers: %d  tasks: %d \n", numWorkers, count)
//	fmt.Printf("\n\nCompleted %d tasks in %v\n", count, elapsed)
//	fmt.Printf("Average rate: %.2f requests/second\n", float64(count)/elapsed.Seconds())
//	fmt.Printf("Failed requests: %d (%.2f%%)\n", failed, float64(failed)/float64(count)*100)
//}
