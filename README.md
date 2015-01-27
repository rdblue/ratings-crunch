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

### Average ratings using Impala

First we need to tell Impala to refresh its metastore so the new `ratings` table will be
visible:

```
impala-shell -q 'invalidate metadata ratings'
```

Then we can issue queries:

```
impala-shell -q 'select movie_id, avg(rating) from ratings group by movie_id'
```