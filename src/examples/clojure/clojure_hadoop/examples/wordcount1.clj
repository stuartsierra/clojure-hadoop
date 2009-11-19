;; wordcount1 -- low-level MapReduce example
;;
;; This namespace demonstrates how to use the lower layers of
;; abstraction provided by the clojure-hadoop library.
;;
;; This is the example word count program used in the Hadoop MapReduce
;; tutorials.  As you can see, it is very similar to the Java code, and
;; uses the Hadoop API directly.
;;
;; We have to call gen-job-classes and gen-main-method, then define the
;; three functions mapper-map, reducer-reduce, and tool-run.
;;
;; To run this example, first compile it (see instructions in
;; README.txt), then run this command (all one line):
;;
;;   java -cp examples.jar \
;;        clojure_hadoop.examples.wordcount1 \
;;        README.txt out1
;;
;; This will count the instances of each word in README.txt and write
;; the results to out1/part-00000
 
  
(ns clojure-hadoop.examples.wordcount1
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp])
  (:import (java.util StringTokenizer)
           (org.apache.hadoop.util Tool)))

(imp/import-io)     ;; for Text, LongWritable
(imp/import-fs)     ;; for Path
(imp/import-mapred) ;; for JobConf, JobClient

(gen/gen-job-classes)  ;; generates Tool, Mapper, and Reducer classes
(gen/gen-main-method)  ;; generates Tool.main method

(defn mapper-map
  "This is our implementation of the Mapper.map method.  The key and
  value arguments are sub-classes of Hadoop's Writable interface, so
  we have to convert them to strings or some other type before we can
  use them.  Likewise, we have to call the OutputCollector.collect
  method with objects that are sub-classes of Writable."
  [this key value #^OutputCollector output reporter]
  (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
    (.collect output (Text. word) (LongWritable. 1))))

(defn reducer-reduce 
  "This is our implementation of the Reducer.reduce method.  The key
  argument is a sub-class of Hadoop's Writable, but 'values' is a Java
  Iterator that returns successive values.  We have to use
  iterator-seq to get a Clojure sequence from the Iterator.  

  Beware, however, that Hadoop re-uses a single object for every
  object returned by the Iterator.  So when you get an object from the
  iterator, you must extract its value (as we do here with the 'get'
  method) immediately, before accepting the next value from the
  iterator.  That is, you cannot hang on to past values from the
  iterator."
  [this key values #^OutputCollector output reporter]
  (let [sum (reduce + (map (fn [#^LongWritable v] (.get v)) (iterator-seq values)))]
    (.collect output key (LongWritable. sum))))

(defn tool-run
  "This is our implementation of the Tool.run method.  args are the
  command-line arguments as a Java array of strings.  We have to
  create a JobConf object, set all the MapReduce job parameters, then
  call the JobClient.runJob static method on it.

  This method must return zero on success or Hadoop will report that
  the job failed."
  [#^Tool this args]
  (doto (JobConf. (.getConf this) (.getClass this))
    (.setJobName "wordcount1")
    (.setOutputKeyClass Text)
    (.setOutputValueClass LongWritable)
    (.setMapperClass (Class/forName "clojure_hadoop.examples.wordcount1_mapper"))
    (.setReducerClass (Class/forName "clojure_hadoop.examples.wordcount1_reducer"))
    (.setInputFormat TextInputFormat)
    (.setOutputFormat TextOutputFormat)
    (FileInputFormat/setInputPaths (first args))
    (FileOutputFormat/setOutputPath (Path. (second args)))
    (JobClient/runJob))
  0)
