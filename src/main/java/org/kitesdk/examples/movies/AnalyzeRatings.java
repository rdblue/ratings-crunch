/**
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.examples.movies;

import java.io.Serializable;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.crunch.CrunchDatasets;

public class AnalyzeRatings extends CrunchTool implements Serializable {

  public AnalyzeRatings(boolean inMemory) {
    super(inMemory);
  }

  @Override
  public int run(String[] args) throws Exception {
    Dataset<Rating> ratings = Datasets.load("dataset:hive:ratings", Rating.class);

    PCollection<Rating> collection = read(CrunchDatasets.asSource(ratings));
    PTable<Long, Double> table = collection
        .by(new GetMovieID(), Avros.longs())
        .mapValues(new GetRating(), Avros.ints())
        .groupByKey()
        .mapValues(new AverageRating(), Avros.doubles());

    writeTextFile(table, "average_ratings");

    return getPipeline().done().succeeded() ? 0 : 1;
  }

  public static class GetMovieID extends MapFn<Rating, Long> {
    @Override
    public Long map(Rating rating) {
      return rating.getMovieId();
    }
  }

  public static class GetRating extends MapFn<Rating, Integer> {
    @Override
    @SuppressWarnings("unchecked")
    public Integer map(Rating rating) {
      return rating.getRating().intValue();
    }
  }

  public static class AverageRating extends MapFn<Iterable<Integer>, Double> {
    @Override
    public Double map(Iterable<Integer> ratings) {
      long sum = 0;
      int count = 0;
      for (Integer rating : ratings) {
        sum += rating;
        count += 1;
      }
      return ((double) sum) / count;
    }
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new AnalyzeRatings(false), args);
    System.exit(rc);
  }

}
