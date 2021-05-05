# MIT-6.824-spring2021
MIT-6.824-spring2021, lab implmentations , includes lab 2A 2B 2C 2D 3A 3B  
Raft with golang  

## Recent
4A 4B to be finished...  

## Usage
### Lab2
cd src/raft  
  
#Single round test  
go test -run=2A -race  
  
#100 round test with up to 2 test programs running concurrently  
python3 dtest.py -n 100 -p 2 2D  

### Lab3
cd src/kvraft  
Other instructions are the same as lab2  

## Correctness
I have passed over 10k rounds of test in lab2   
I have passed over 100 rounds of test in lab3 (and I dont want to run it again, since lab3 is much more easier than lab2 but takes much more time for testing)  
  
All above test without -race flag, may still have data contention.  
But I tried run 3B with -race once and passed, so it should be right, I guess so but not promise  
Good luck to you!  


## Enable Logger
We have prevent some logs from printing  
If you want to enable it  
Go to src/raft-logs/log-common.go   
Inside function init()   
Add log topics to print_list  
Also you can add more logTopic definitions as you like
```  
print_list := []LogTopic{
		//Clerk,
		//Server,
		//Leader,
}
```  

## Any other question just email me 1291463831@qq.com



