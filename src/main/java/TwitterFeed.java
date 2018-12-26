import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import twitter4j.Status;
import java.util.Arrays;
import java.util.regex.Pattern;

public class TwitterFeed {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // twitter app credentials- set as system variables
        // create SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("Twitter Streaming Data handling")
                .setMaster("local[4]")
                .set("spark.executor.memory", "1g");
        // create JavaStreamingContext object
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,  new Duration(5000));

        //to stop displaying messages on Spark console
        Logger.getLogger("org").setLevel(Level.OFF);

        //The object tweets is a DStream of tweet statuses.
        JavaDStream<Status> tweets = TwitterUtils.createStream(jsc);

        //the map operation on tweets maps each Status object to its text to create a new ‘transformed’ DStream named statuses.
        JavaDStream<String> statuses = tweets.map(
                t ->t.getText()
        );

        // Q1
        statuses.foreachRDD(tweetT -> {
            tweetT.foreach(t -> {
                        //Prints tweet itself.
                        System.out.println("Tweet:");
                        System.out.println();
                        System.out.println(t);
                        System.out.println();
                        System.out.println("-------------------------------------------------------");
                        // count of words and character length can be done here also
            });
        });

        // get number of characters
        JavaDStream<Integer> tweetLen = statuses.map(
                v -> v.length() // considering even spaces to be a different character
        );
        tweetLen.print(); //printing the first ten records
        JavaDStream<Integer> wordLen = statuses.map(
                v -> v.split(" ").length // calculating the count of number of words
        );
        wordLen.print(); // printing the first ten records
        statuses.foreachRDD(rdd -> {
            Double averageCharLength = rdd.mapToDouble(
                    v -> v.length() // consider the character length
            ).mean(); //computing the average of all the values
            System.out.println("Average Tweet character length  is "+ averageCharLength.toString());
            Double averageNumWords = rdd.mapToDouble(
                    s -> s.split(" ").length // considering the count of words
            ).mean();
            System.out.println("Average Number of words is "+ averageNumWords.toString());
        });
        JavaDStream<String> words = statuses.flatMap(
                (String s) -> Arrays.asList(SPACE.split(s)).iterator()); // extracting all the words from the tweets
        //apply the filter function to retain only the hashtags
        JavaDStream<String> hashTags = words.filter( t -> t.startsWith("#")); // extracting the hastags
        //counting the number of hashTags
        // Getting Top 10 tweets
        JavaPairDStream<String, Integer> tuples =
                hashTags.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1)); // mapping each hashtag to a pair so that it can be counted

        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
                (Integer i1, Integer i2) -> i1 + i2, // for every slide of 1 second, count of new hashtags is increased
                (Integer i1, Integer i2) -> i1 - i2,// for every slide of 1 second, count of old hashtags is decreased
                new Duration(1* 1000), // window duration 1 second
                new Duration(1 *1000) //slide duration 1 second
        );
        // 3b top 10 hashtags
        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(x-> x.swap());
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(in -> in.sortByKey(false));

        sortedCounts.foreachRDD(rdd -> {
            String out = "\nTop 10 hashtags:\n";
            // get the top 10 hashtags and print them
            for (Tuple2<Integer, String> t: rdd.take(10)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);

        });

        statuses.window(Durations.seconds(5*60), Durations.seconds(30)).foreachRDD(rdd -> {
            Double averageCharLength = rdd.mapToDouble(
                    v -> v.length()
            ).mean();
            System.out.println("Average Tweet character length for the last 5 minutes of tweets is " + averageCharLength.toString());
            Double averageNumWords = rdd.mapToDouble(
                    s -> s.split(" ").length
            ).mean();
            System.out.println("Average Number of words  for the last 5 minutes of tweets is "+ averageNumWords.toString());
        });
        sortedCounts.window(Durations.seconds(5*60), Durations.seconds(30)).foreachRDD(rdd -> {
            String out = "\nTop 10 hashtags for the last 5 minutes of tweets: \n";
            for (Tuple2<Integer, String> t: rdd.take(10)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);

        });
        jsc.checkpoint("checkpoint"); //checkpoints are saved in checkpoint directory
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            // can be logged
        }
        jsc.stop();
    }

}
