# go-file-dedupe

## Attribution 
Based on https://github.com/imdawon/go-file-dedupe 

This is not a dedup system yet. It only scans the current working directory for now.
The system is architected to use works for perform the digesting.

## What does this software do?
go-file-dedupe recursively searches the current working directory. It uses sha256 ( or MD5 ) to hash files to identify duplicates. 


**DISCLAIMER:
You assume full responsibility of the side effects that may occur by running this program. I am not responsibile for any intentional / accidental data loss.
This software also has no concept of which file is the "original". It may not find your files in order, so it may delete one from the folder you're familiar with it in. If you have a preference for maintaining file location for the "original" file, then you shouldn't use this software.**

## Progress
It uses a goroutine to print out stats as it runs.
It can digest a file in the CWD tree using sha256 or md5.
It can use goroutines to compute digests. The count is configurable
It reports common files without removing the duplicates yet.

## To Do
Handle symlinks.
Link rather than remove.
Experiment with CAS like git does.

Nicky
