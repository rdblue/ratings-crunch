## Movie Ratings Analysis

This example shows a movie ratings web application that logs rating events to
HDFS for follow-on analysis. Start by running the [web application][ratings-app].

[ratings-app]: https://github.com/rdblue/ratings-app

## Setup

These instructions are intended for the [QuickStart VM][quickstart-vm], but
should work on most Hadoop clusters. Start by cloning this repository:

```
git clone https://github.com/rdblue/ratings-crunch.git
cd ratings-crunch
```

### Average ratings using Crunch

```
mvn package
mvn kite:run-tool
hadoop fs -cat average_ratings/part-r-00000
```


