# WordCount

This exercise performs classical word count task from Apache Hadoop MapReduce.

## Instructions

To compile the code:

```
$ mvn compile
```

To create a jar package:
```
$ mvn package
```

To execute the code:
```
$ yarn jar target/wordcount-1.0-SNAPSHOT.jar es.deusto.bdp.hadoop.wordcount.WordCount input/ output/
```

The output must have the following format, e.g.:
```
the,13
a,14
I,95
...
```
