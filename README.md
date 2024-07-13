# laptop-big-csv-col-deduplicate
playground with dask and modin




##  daset


## modin


2024-07-13 20:30:16,902	INFO worker.py:1771 -- Started a local Ray instance.
Reading CSV file...
Splitting URLs and creating deduplication key...
Removing duplicates...
Saving deduplicated data...
Total rows: 6,006,448
Unique URLs (based on second part): 4,043,444
Removed 1,963,004 duplicate rows
Time taken for asynchronous execution with concurrency limited by semaphore: 40.89457178115845 seconds


## daask

[########################################] | 100% Completed | 44.90 s
Total rows: 6,006,448
Unique URLs: 4,043,444
Removed 1,963,004 duplicate rows
Time taken for asynchronous execution with concurrency limited by semaphore: 82.19625806808472 seconds
