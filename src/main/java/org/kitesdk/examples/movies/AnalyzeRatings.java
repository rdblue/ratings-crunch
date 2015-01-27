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
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.TupleN;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.crunch.CrunchDatasets;

import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.longs;
import static org.apache.crunch.types.avro.Avros.tuples;

public class AnalyzeRatings extends CrunchTool implements Serializable {

  public AnalyzeRatings(boolean inMemory) {
    super(inMemory);
  }

  @Override
  public int run(String[] args) throws Exception {
    Dataset<Rating> ratings = Datasets.load("dataset:hive:ratings", Rating.class);

    PCollection<Rating> collection = read(CrunchDatasets.asSource(ratings));
    PTable<Long, TupleN> table = collection
        .by(new GetMovieID(), longs())
        .mapValues(new GetRating(), ints())
        .groupByKey()
        .mapValues(new BuildRatingsHistogram(),
            tuples(ints(), ints(), ints(), ints(), ints()))
        .filter(new IdentifyBimodalMovies());

    writeTextFile(table, "bimodal_movies");

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
    public Integer map(Rating rating) {
      return rating.getRating();
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

  public static class BuildRatingsHistogram extends MapFn<Iterable<Integer>, TupleN> {
    @Override
    public TupleN map(Iterable<Integer> ratings) {
      int[] counts = new int[] {0, 0, 0, 0, 0};
      for (int rating : ratings) {
        if (rating <= 5) {
          counts[rating - 1] += 1;
        }
      }
      return new TupleN(counts[0], counts[1], counts[2], counts[3], counts[4]);
    }
  }

  // require a difference of at least 2 to register a change
  public static final int CHANGE_THRESH = 2;

  public static class IdentifyBimodalMovies extends FilterFn<Pair<Long, TupleN>> {
    @Override
    public boolean accept(Pair<Long, TupleN> movieRatingHistograms) {
      TupleN ratingsHistogram = movieRatingHistograms.second();
      int last = -1;
      int totalRatings = 0;
      boolean foundNegative = false;
      boolean foundBimodal = false;
      for (Object countObj : ratingsHistogram.getValues()) {
        int count = (Integer) countObj;
        totalRatings += count;

        if (last >= 0) {
          int diff = count - last;
          if (foundNegative && diff > CHANGE_THRESH) {
            foundBimodal = true;
          }
          if (diff < -CHANGE_THRESH) {
            foundNegative = true;
          }
        }

        last = count;
      }

      // filter out some noise
      if (totalRatings < 20) {
        return false;
      }

      return foundBimodal;
    }
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new AnalyzeRatings(false), args);
    System.exit(rc);
  }

}
