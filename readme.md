## Removing duplicate arguments from args.me corpus
Arguments are considered duplicates if they have the same id or the same text (premise and conclusion)

### execution
To run the script, excute 
```
./deploy/clean_args_me.sh
```

The script will 
- remove duplicated arguments with the same id
- remove duplicated arguments with the same text
- save a cleaned version in {cleaned-hash}

### configuration
input and output parameters are found under 
```
./conf/args-me.conf
```
source:         path of a folder of args.me json files

preprocessed:   path of a csv file with args.me arguments cleaned and hashed

cleaned-id:     path of a folder of args.me json files with arguments with the same ids removed 

cleaned-hash:   path of a folder of args.me json with arguments with the same hash removed


