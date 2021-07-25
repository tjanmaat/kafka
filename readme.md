# Kafka Introduction
This project contains a python simple application that draws and transforms data from a stream with data
 on the latest changes to Wikipedia.
It was made to get familiar with Kafka and Spark, which are used in this project. 
Some notes on the process of making this project can be found in `notebook/process_notes.ipynb`. 

## Usage
This project runs in a (few) Docker containers. 
To run it, please install [Docker](https://docs.docker.com/get-docker/) first.

To use this project, clone the repository and run the bash script in the root folder:
```
$ run.sh
```
This script builds a Docker image and starts a few Docker containers, 
in which the scripts that run Kafka and Spark are started.

This project was built and tested in Windows 10. 
Due to the usage of Docker, it should work in other systems too.

## Maintenance
This project is built for personal use. 
Feel free to use it however you see fit. 
However, it will not be maintained and contribution will not be accepted.