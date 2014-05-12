Automatically execute a bunch of tests for Stratosphere in a distribted setting
####

# Preparation

Understand that I'm following a requirements-based development process.
It is likely that a special corner case is not covered, our that my use case is actually a corner case.
Anyways, please let me know if something is missing for you or make a pull request.

# Required applications
- bash
- git
- maven 3

for wordcount data:
- aspell
- ruby
- iconv

for tpch:
- make
- a c compiler (gcc)

```
sudo apt-get install git maven aspell ruby make
```

# Execution Order 

```
#set values here
nano config.sh
./prepareStratosphere.sh
./prepareTestjob.sh
./generateWCdata.sh
./uploadToHdfs.sh
./startStratosphere.sh

# now you can run a job
./runWC.sh
```
