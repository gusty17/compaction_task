Gate the merge on the raw XML. The parsed output 
is derived entirely from xmlrecord, so if 
the XML didn't change, the parsed row can't
have changed either. Filter the source to only 
changed recids before parsing — that saves the 
parsing work too, not just the file churn.


The metadata/ folder sits at 40 files / 1.7 MB and neither step touched it — it's now larger than your actual data. Manifest and metadata files accumulate separately, and these three statements don't address them. Not urgent at your scale, but it grows with every run.

Want me to fix the compact.sql comments to match what we just measured, and look into the metadata cleanup?


rewrite a file if:  size < threshold   OR   it has delete files pointing into it
optimize reads the existing files and rewrites any file that's either under the size threshold (100 MB) or has delete files pointing into it, bin-packing the qualifying ones up toward the target size (512 MB) into new file(s). Untouched files keep their existing manifest entries unchanged. A new snapshot is created pointing at a new manifest-list, which mixes new manifests (for what was rewritten) with reused old ones (for what wasn't) — and MinIO keeps every old file physically in place, since nothing is ever deleted at this step.