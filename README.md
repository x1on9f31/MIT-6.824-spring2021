# MIT-6.824-spring2021
MIT-6.824-spring2021, lab implmentations , includes lab 2A 2B 2C 2D 3A 3B

## Usage
lab2:  
cd src/raft  
  
#singal test 
go test -run=2A -race  
  
#100 round test with up to 2 test programs running concurrently  
python3 dtest.py -n 100 -p 2 2D  

lab3:  
cd src/kvraft  
other instrutions are same as lab2  

## Warning
we have prevent some logs from printing  
if you want to enable them  
goto src/raft/logger.go  
set these const variables to false  
```  
RAFT_IGNORE    = false
COMMIT_IGNORE  = false //apply,commit
TIMER_IGNORE   = true  //timer
LEADER_IGNORE  = true  //leader
APPEND_IGNORE  = true  //append
ROLE_IGNORE    = false //term vote
PERSIST_IGNORE = false //persist log
```  

## Any other question just email me 1291463831@qq.com



