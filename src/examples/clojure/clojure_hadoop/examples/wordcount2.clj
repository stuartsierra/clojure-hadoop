;; wordcount2 -- wrapped MapReduce example
;;
;; This namespace demonstrates how to use the function wrappers
;; provided by the clojure-hadoop library.
;;
;; As in the wordcount1 example, we have to call gen-job-classes and
;; gen-main-method, then define the three functions mapper-map,
;; reducer-reduce, and tool-run.
;;
;; mapper-map uses the wrap-map function.  This allows us to write our
;; reducer as a simple, pure-Clojure function.  Converting between
;; Hadoop types, and dealing with the Hadoop APIs, are handled by the
;; wrapper.  We give it a function that returns a sequence of pairs,
;; and a pre-defined reader that accepts a Hadoop [LongWritable, Text]
;; pair.  The default writer function writes keys and values as Hadoop
;; Text objects rendered with pr-str.
;;
;; reducer-reduce similarly uses the wrap-reduce function.  However,
;; rather than passing the sequence of values directly to the
;; function, wrap-reduce will pass a *function* that *returns* a lazy
;; sequence of values.  Because this sequence may be very large, you
;; must be careful never to bind it to a local variable.  Basically,
;; you should only use the values-fn in one of Clojure's sequence
;; functions such as map, filter, or reduce.
;;
;; To run this example, first compile it (see instructions in
;; README.txt), then run this command (all one line):
;;
;;   java -cp examples.jar \
;;        clojure_hadoop.examples.wordcount2 \
;;        README.txt out2
;;
;; This will count the instances of each word in README.txt and write
;; the results to out2/part-00000
;;
;; Notice that, in the output file, the words are enclosed in double
;; quotation marks.  That's because they are being printed as readable
;; strings by Clojure, as with 'pr'.


(ns clojure-hadoop.examples.wordcount2
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.wrap :as wrap])
  (:import (java.util StringTokenizer)
           (org.apache.hadoop.util Tool)))

(imp/import-io)     ;; for Text
(imp/import-fs)     ;; for Path
(imp/import-mapred) ;; for JobConf, JobClient

(gen/gen-job-classes)
(gen/gen-main-method)

(def mapper-map
     (wrap/wrap-map 
      (fn [key value]
        (map (fn [token] [token 1])
             (enumeration-seq (StringTokenizer. value))))
      wrap/int-string-map-reader))

(def reducer-reduce
     (wrap/wrap-reduce
      (fn [key values-fn]
        [[key (reduce + (values-fn))]])))

(defn tool-run [#^Tool this args]
  (doto (JobConf. (.getConf this) (.getClass this))
    (.setJobName "wordcount2")
    (.setOutputKeyClass Text)
    (.setOutputValueClass Text)
    (.setMapperClass (Class/forName "clojure_hadoop.examples.wordcount2_mapper"))
    (.setReducerClass (Class/forName "clojure_hadoop.examples.wordcount2_reducer"))
    (.setInputFormat TextInputFormat)
    (.setOutputFormat TextOutputFormat)
    (FileInputFormat/setInputPaths #^String (first args))
    (FileOutputFormat/setOutputPath (Path. (second args)))
    (JobClient/runJob))
  0)
