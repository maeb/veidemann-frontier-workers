/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"

	"github.com/nlnwa/veidemann-frontier-workers/database"
	"github.com/rs/zerolog/log"
)

// worker is a function that may return an error.
type worker func() error

// waitQueueWorker returns a worker that moves crawl host groups from wait to ready queue.
func waitQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveWaitToReady(); err != nil {
			return fmt.Errorf("error moving crawl host groups from wait queue to ready queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl host group(s) is ready", moved)
		}
		return nil
	}
}

// busyQueueWorker returns a worker that moves crawl host groups from busy to timeout queue.
func busyQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveBusyToTimeout(); err != nil {
			return fmt.Errorf("error moving crawl host groups from busy queue to timeout queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl host group(s) timed out", moved)
		}
		return nil
	}
}

// executionTimeoutQueueWorker returns a worker that moves crawl executions from running to timeout queue.
func executionTimeoutQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveRunningToTimeout(); err != nil {
			return fmt.Errorf("error moving crawl executions from running to timeout queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl execution(s) timed out", moved)
		}
		return nil
	}
}

// updateJobExecutions returns a worker that updates rethinkdb job execution based on redis hashmap.
func updateJobExecutions(db database.Database) worker {
	return func() error {
		if count, err := db.UpdateJobExecutions(context.Background()); err != nil {
			return fmt.Errorf("failed to update job executions: %w", err)
		} else if count > 0 {
			log.Debug().Msgf("Updated %d job execution(s)", count)
		}
		return nil
	}
}

// removeUriQueueWorker returns a worker that deletes up to 10000 queued uris per batch from rethinkdb.
func removeUriQueueWorker(db database.Database) worker {
	return func() error {
		if removed, err := db.RemoveFromUriQueue(context.Background()); err != nil {
			return err
		} else if removed > 0 {
			log.Debug().Msgf("Removed %d queued uris", removed)
		}
		return nil
	}
}

// crawlExecutionTimeoutWorker returns a worker that sets desired state to ABORTED_TIMOUT en crawl exectutions in timeout queue.
func crawlExecutionTimeoutWorker(db database.Database) worker {
	return func() error {
		if timeouts, err := db.TimeoutCrawlExecutions(context.Background()); err != nil {
			return fmt.Errorf("time out crawl executions: %w", err)
		} else if timeouts > 0 {
			log.Debug().Msgf("%d crawl execution(s) timed out", timeouts)
		}
		return nil
	}
}
