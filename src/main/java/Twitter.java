import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import scala.*;
import java.lang.Boolean;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.util.HashMap;
import java.util.Arrays;

//import io.fabric8.kubernetes.api.model.Status;
//import mx4j.log.Log4JLogger;

public class Twitter {

    public static void main(String[] args) throws Exception {
        //assigning consumer key , consumer secret,access token and accesstokensecret
        String consumerKey = "Wv30reHMR8xkjoPefbgprzbtJ ";
        String consumerSecret = "nmcdlJNMrv0nWgZJmO84AjdOaxlJQZ6h7ck9OQzZ6lpnY5fHM2 ";
        String accessToken = "1065725575103504389-6wQaxYYdg3XDKKiU4r9Qaoz32HeWsM";
        String accessTokenSecret = "p8Oz5U8iW95ut0hh7LjTR2BJyUdBMPwDI0enGbuuqV3sV";

        //set master and app name using sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("Twitter").setMaster("local[4]").set("spark.ui.port", "44040");
        //to retrieve every second of tweet
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

//        Logger.getLogger("new").setLevel(Level.ALL);
 //       Logger.getLogger("all").setLevel(Level.ALL);
        Logger.getRootLogger().setLevel(Level.ERROR);
        //checkpoint
        ssc.checkpoint("/tmp/");
        //assigning key and value using hash function
        final HashMap<String, String> configs = new HashMap<String, String>();
        configs.put("apiKey", consumerKey);
        configs.put("apiSecret", consumerSecret);
        configs.put("accessToken", accessToken);
        configs.put("accessTokenSecret", accessTokenSecret);
        //since the above might not consider string data the below function is written to read the key and token values

        final Object[] keys = configs.keySet().toArray();
        for (int k = 0; k < keys.length; k++) {
            final String key = keys[k].toString();
            final String value = configs.get(key).trim();
            if (value.isEmpty()) {
                throw new Exception("Error setting authentication - value for " + key + " not set");
            }
            final String fullKey = "twitter4j.oauth." + key.replace("api", "consumer");
            System.setProperty(fullKey, value);
        }


        //given
        //TutorialHelper.configureTwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);
        //JavaDStream<Status> tweets1 = jssc.twitterStream();
        JavaDStream<String> statuses = tweets.map(t -> t.getText()); //map function retreive the text
        statuses.print();

        // 2nd question
        JavaDStream<Integer> tweetLength = statuses.map(
                s -> s.toString().length()
        );
        tweetLength.print(); // to count the number of character

        JavaDStream<Integer> wordLen = statuses.map(
                s-> s.split(" ").length
        );
        wordLen.print(); // to count the number of word
        JavaDStream<String> words = statuses.flatMap(
                (String s) -> Arrays.asList(s.split(" ")).iterator()
        );
        JavaDStream<String> hashTags = words.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String word) { return word.startsWith("#"); }    //to determine tweets that has hastags '#;
                }
        );


        //3rd question
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String in) {
                        return new Tuple2<String, Integer>(in, 1);
                    }
                }
        );

        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) { return i1 + i2; }
                },
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) { return i1 - i2; }
                },
                new Duration(60 * 5 * 1000), //to determine tweets of last 5 minutes for every 30 seconds
                new Duration(30 * 1000)
        );
        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer,String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
                        return in.swap();
                    }
                }
        );

        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
                new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
                    public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
                        return in.sortByKey(false);
                    }
        });

        sortedCounts.foreachRDD(rdd -> {
            String hashtags = "\nTop 10 hashtags:\n";
            // get the top 10 hashtags and print them
            for (Tuple2<Integer, String> t: rdd.take(10)) {
                hashtags = hashtags + t.toString() + "\n";
            }
            System.out.println(hashtags);

        });
        ssc.start();
        ssc.awaitTermination();

    }


}