# go-file-dedupe

Recursively search for duplicate files from the binary's working directory. Uses sha256 to hash files and detect dupes. 

In personal testing it removed 16470 duplicate files from a 150GB pool of files in the span of ~30 minutes.

## Example output
![dedupe cli output](dedupe-dialog.png)
