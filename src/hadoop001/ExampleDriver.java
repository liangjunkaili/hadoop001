package hadoop001;

import org.apache.hadoop.util.ProgramDriver;

import hadoop001.mapreduce.UserHitsCount;
import hadoop001.mapreduce.WordCount;

public class ExampleDriver
{
  public static void main(String[] argv)
  {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
      pgd.addClass("userhits", UserHitsCount.class, "");
      /*pgd.addClass("wordmean", WordMean.class, "A map/reduce program that counts the average length of the words in the input files.");

      pgd.addClass("wordmedian", WordMedian.class, "A map/reduce program that counts the median length of the words in the input files.");

      pgd.addClass("wordstandarddeviation", WordStandardDeviation.class, "A map/reduce program that counts the standard deviation of the length of the words in the input files.");

      pgd.addClass("aggregatewordcount", AggregateWordCount.class, "An Aggregate based map/reduce program that counts the words in the input files.");

      pgd.addClass("aggregatewordhist", AggregateWordHistogram.class, "An Aggregate based map/reduce program that computes the histogram of the words in the input files.");

      pgd.addClass("grep", Grep.class, "A map/reduce program that counts the matches of a regex in the input.");

      pgd.addClass("randomwriter", RandomWriter.class, "A map/reduce program that writes 10GB of random data per node.");

      pgd.addClass("randomtextwriter", RandomTextWriter.class, "A map/reduce program that writes 10GB of random textual data per node.");

      pgd.addClass("sort", Sort.class, "A map/reduce program that sorts the data written by the random writer.");

      pgd.addClass("pi", QuasiMonteCarlo.class, "A map/reduce program that estimates Pi using a quasi-Monte Carlo method.");
      pgd.addClass("bbp", BaileyBorweinPlouffe.class, "A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact digits of Pi.");
      pgd.addClass("distbbp", DistBbp.class, "A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.");

      pgd.addClass("pentomino", DistributedPentomino.class, "A map/reduce tile laying program to find solutions to pentomino problems.");

      pgd.addClass("secondarysort", SecondarySort.class, "An example defining a secondary sort to the reduce.");

      pgd.addClass("sudoku", Sudoku.class, "A sudoku solver.");
      pgd.addClass("join", Join.class, "A job that effects a join over sorted, equally partitioned datasets");
      pgd.addClass("multifilewc", MultiFileWordCount.class, "A job that counts words from several files.");
      pgd.addClass("dbcount", DBCountPageView.class, "An example job that count the pageview counts from a database.");
      pgd.addClass("teragen", TeraGen.class, "Generate data for the terasort");
      pgd.addClass("terasort", TeraSort.class, "Run the terasort");
      pgd.addClass("teravalidate", TeraValidate.class, "Checking results of terasort");
*/      exitCode = pgd.run(argv);	
    }
    catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}