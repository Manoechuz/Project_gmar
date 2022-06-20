# Project_gmar
 
Welcome to the project "Migrating ERP system to BI with Cassandra". In this following branch you can see the code of our benchmarks that we had build.

## Workload Generator :

This programs must run on python 3 use the file "generator.py" it will ask you the number of rows you want and will generate a workload.

## Timestamp Benchmark :

There is here 2 main programs :
- For the benchmark of Cassandra and SQLite without Timestamp use the file: "Without_Timestamp_Benchmark.py"
- For the benchmark of Cassandra and SQLite with Timestamp use the file: "With_Timestamp_Benchmark.py

Make sure that before the run you have install Cassandra and SQLite and generate data or download the workloads file given here, and that you have in your databases created the table "persons" with his features, you can create him by using line of code in the programs or create directly on the terminal.

## SQLite Benchmark :

Must have a workload ready before launch you can use the program "Sql_perf_bench.py" to launch a benchmark.

## Cassandra-Stress:

You will find here the YAML file for the user mode you can run a benchmark in the cassandra folder (your own) in the path "Cassandra/tools/bin" the name of YAML file is "benchcass.yml"



### Thank you and good day.


