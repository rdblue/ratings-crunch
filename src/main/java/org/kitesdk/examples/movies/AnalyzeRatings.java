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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Collection;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.crunch.CrunchDatasets;

import static org.apache.crunch.types.avro.Avros.collections;
import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.longs;
import static org.apache.crunch.types.avro.Avros.specifics;

public class AnalyzeRatings extends CrunchTool implements Serializable {

  public static final String IN_DATASET_URI = "dataset:hive:ratings";
  public static final String OUT_DATASET_URI = "dataset:hive:ratings_histograms";

  public AnalyzeRatings(boolean inMemory) {
    super(inMemory);
  }

  @Override
  public int run(String[] args) throws Exception {
    // load or create the output dataset
    Dataset<MovieRatingsHistogram> histograms;
    if (Datasets.exists(OUT_DATASET_URI)) {
      histograms = Datasets.load(OUT_DATASET_URI,
          MovieRatingsHistogram.class);
    } else {
      histograms = Datasets.create(OUT_DATASET_URI,
          new DatasetDescriptor.Builder()
              .schema(MovieRatingsHistogram.class)
              .build(),
          MovieRatingsHistogram.class);
    }

    PCollection<Rating> ratings = read(CrunchDatasets.asSource(
        Datasets.load(IN_DATASET_URI, Rating.class)));

    PCollection<MovieRatingsHistogram> bimodal = ratings
        .by(new GetMovieID(), longs())
        .mapValues(new GetRating(), ints())
        .groupByKey()
        .mapValues(new AccumulateRatings(), collections(ints()))
        .parallelDo(new BuildRatingsHistogram(),
            specifics(MovieRatingsHistogram.class))
        .filter(new IdentifyBimodalMovies());

    bimodal.write(
        CrunchDatasets.asTarget(histograms),
        Target.WriteMode.OVERWRITE);

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
      return rating.getRating().intValue();
    }
  }

  public static class AccumulateRatings
      extends MapFn<Iterable<Integer>, Collection<Integer>> {
    @Override
    public Collection<Integer> map(Iterable<Integer> ratings) {
      int[] counts = new int[] {0, 0, 0, 0, 0};
      for (int rating : ratings) {
        if (rating <= 5) {
          counts[rating - 1] += 1;
        }
      }
      return Lists.newArrayList(
          counts[0], counts[1], counts[2], counts[3], counts[4]);
    }
  }

  public static class BuildRatingsHistogram
      extends MapFn<Pair<Long, Collection<Integer>>, MovieRatingsHistogram> {
    @Override
    public MovieRatingsHistogram map(Pair<Long, Collection<Integer>> movieIdAndRatings) {
      return new MovieRatingsHistogram(movieIdAndRatings.first(),
          Lists.newArrayList(movieIdAndRatings.second()));
    }
  }

  // require a difference of at least 2 to register a change
  public static final int CHANGE_THRESH = 2;

  public static class IdentifyBimodalMovies extends FilterFn<MovieRatingsHistogram> {
    @Override
    public boolean accept(MovieRatingsHistogram movieRatingHistogram) {
      int last = -1;
      int totalRatings = 0;
      boolean foundNegative = false;
      boolean foundBimodal = false;

      for (Integer count : movieRatingHistogram.getHistogram()) {
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
