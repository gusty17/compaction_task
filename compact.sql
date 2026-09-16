-- Step 1: merge the small files into large ones, and apply the merge-on-read
-- strategy to reduce the number of files. This also deletes files so they can be dropped. This is the step that makes queries
-- fast again. It rewrites files below the file_size_threshold (default 100MB).
ALTER TABLE __TABLE__ EXECUTE optimize;

--step 2: expire snapshots older than the retention threshold (default 30 days).
ALTER TABLE __TABLE__ EXECUTE expire_snapshots(retention_threshold => '__RETENTION__');

--step 3: remove orphan files that are not referenced by any snapshot. This is the step that actually deletes files from the file system.
ALTER TABLE __TABLE__ EXECUTE remove_orphan_files(retention_threshold => '__RETENTION__');
