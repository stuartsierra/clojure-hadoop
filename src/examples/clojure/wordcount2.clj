;; wordcount2 -- wrapped MapReduce example
;;
;; This namespace demonstrates how to use the function wrappers
;; provided by the clojure-hadoop library.
;;
;; As in the wordcount1 example, we have to call gen-job-classes and
;; gen-main-method, then define the three functions mapper-map,
;; reducer-reduce, and tool-run.
;;
;; Here, mapper-map is identical to wordcount1, with the exception that
;; we output keys and values as strings created with pr-str.
;;
;; But reducer-reduce uses the wrap-reduce function.  This allows us to
;; write our reducer as a simple, pure-Clojure function.  Converting
;; between Hadoop types, and dealing with the Hadoop APIs, are handled
;; by the wrapper.
;;
;; The wrappers expect to work with Text values that can be read by the
;; Clojure reader.  That's why the mapper function cannot be wrapped:
;; it is reading a plain text file, not Clojure data structures.
;;
;; To run this example, first compile it (see instructions in
;; README.txt), then run this command:
;;
;;   java -cp examples.jar:lib/* wordcount2 README.txt out2
;;
;; This will count the instances of each word in README.txt and write
;; the results to out2/part-00000
;;
;; Notice that, in the output file, the words are enclosed in double
;; quotation marks.  That's because they are being printed as readable
;; strings by Clojure, as with 'pr'.


(ns wordcount2
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.wrap :as wrap])
  (:import (java.util StringTokenizer)))

(imp/import-io)     ;; for Text, IntWritable
(imp/import-fs)     ;; for Path
(imp/import-mapred) ;; for JobConf, JobClient

(gen/gen-job-classes)
(gen/gen-main-method)

(defn mapper-map [this key value output reporter]
  (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
    (.collect output (Text. (pr-str word)) (Text. (pr-str 1)))))

(def reducer-reduce
     (wrap/wrap-reduce
      (fn [key values]
        [[key (reduce + values)]])))

(defn tool-run [this args]
  (doto (JobConf. (.getConf this) (.getClass this))
    (.setJobName "wordcount2")
    (.setOutputKeyClass Text)
    (.setOutputValueClass Text)
    (.setMapperClass (Class/forName "wordcount2_mapper"))
    (.setReducerClass (Class/forName "wordcount2_reducer"))
    (.setInputFormat TextInputFormat)
    (.setOutputFormat TextOutputFormat)
    (FileInputFormat/setInputPaths (first args))
    (FileOutputFormat/setOutputPath (Path. (second args)))
    (JobClient/runJob))
  0)
