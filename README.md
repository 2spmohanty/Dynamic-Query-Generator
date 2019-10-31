# Dynamic-Query-Generator
Used to generate Queries on Table. All the queries generated are different from each other.

DQG helps you to test your Database compiler, executor codes- how they behave with different unique queries, from the same table.

As the name suggests this is the tool for generating queries for the purpose of functional and stress testing.

The tool was primarly written for HP's SQL/MX Database but it can be tweaked to test other databases too.

### The tool is useful for the following audiences:

Developers can use the tool to test their code and the way it interacts with features of the core database utilizing all permutation and combination of queries. Individual users who are eager to stretch the limit of the database will have a powerful tool in their hands.

Customers who wish to evaluate SQL/MX stability and performance can generate syntactic data and workloads that match the application they are planning to deploy

### How it differs from other similar tools:

1) It reads from meta-data of the table and form queries.
2) It doesn’t require a template file or a grammar file, as is required by other tools to generate queries.
3) User can have control over the projection and predicate lists.
4) Has the option to segregate generated queries in terms of number of queries or file size.
4) Generates millions of unique queries within fraction of seconds.


### Usage

** Values in square bracket are optional parameters.

```

~$ ./dqg.pl [-r 0/1] -t 'table name or t1 joins t2' [-n num_of_columns] [-p projection file] [-w predicate file] [-s 'sub_queries_file'] [-c cqd_settings_file] [-e 0/1] [-o out filename]
[-d 'l: 2000’ / 'b: 1024’ / 'q: 2500’]

```


### Parameter Explanation
```
./dqg.pl   
      -r apart from 0 if set to any value then read from metadata.
			-t table name
			-n number of columns. Ignored when a predicate file is passed. projection file.
			-p projection file
			-w predicate file.
			-s sub queries file.*
			-c cqd_settings_file
			-e apart from 0 if set to any integer, the script generate that many embedded sqls.*
			-o name of output file appended with current time stamp.
			-d if given with option l (l: 2000) split the files on the basis of number of lines specified.
			   If given with option b (b: 1024) split the files on the basis of number of bytes specified.
			   If given with option q (q: 2500) split the files on the basis of number of queries specified.	

```

Use Cases

(1)Case 1: When dqg.dqg.table_name Table has column named as COL[i] where i is an integer less than equal to value given for n.

```
~$ ./dqg.pl -t 'dqg.dqg.table_name' -n 10 -p sample_project_file [-o sample_output_filename]
```

(2)Case 2: When Table name is 'dqg.dqg.table_name' and a ''sample_predicate_file'' is given as the predicate.

```
~$ ./dqg.pl -t 'dqg.dqg.table_name' -p sample_project_file -w sample_predicate_file [-o sample_output_filename]
*** When sample_predicate_file is given [-n num_of_columns] option is ignored.
```

(3)Case 3: When Table name is 'dqg.dqg.table_name' and -r is set to 1.

```
~$ ./dqg.pl -r 1 'dqg.dqg.table_name' -p sample_project_file [-o sample_output_filename]
```
