# MIT-6.824-spring2021
MIT-6.824-spring2021, Raft using golang, includes lab 2A 2B 2C 2D 3A 3B 4A 4B    
 

## Recent
4A finished just now  
4B finished just now  
all lab has finished !! 
time to optimize codes...  

## Usage
### Lab2
cd src/raft  
  
#Single round test  
go test -run=2A -race  
  
#100 round test with up to 2 test programs running concurrently  
python3 dtest.py -n 100 -p 2 2D  

### Lab3
cd src/kvraft   

### Lab4A
cd src/shardctrler  

### Lab4B
cd src/shardkv  

## Correctness
passed over 10k rounds of test in lab2    
passed over 1k rounds of test in lab3   
passed over 10k rounds of test in lab4a   
passed over 50  rounds of test in lab4b

All above test without -race flag, may still have data contention.  
But I tried run 4B with -race once and passed, so it should be right, I guess so but not promise  
Good luck to you!  


## Enable Logger
The logger can take full control of log printing using log topics  

We have prevent some logs from printing  
If you want to enable it  
Go to src/raft-logs/log-common.go   
Inside function init()   
Add log topics to print_list  
Also you can add more logTopic definitions as you like
```  
print_list := []LogTopic{
		//Clerk,
		//ServerApply,
		//Leader,
}
```  

## Any other question just email me 1291463831@qq.com



