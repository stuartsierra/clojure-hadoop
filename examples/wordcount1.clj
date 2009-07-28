(ns wordcount1
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp])
  (:import (java.util StringTokenizer)))

(imp/import-io)     ;; for Text, IntWritable
(imp/import-fs)     ;; for Path
(imp/import-mapred) ;; for JobConf, JobClient

(gen/gen-job-classes)
(gen/gen-main-method)

(defn mapper-map [this key value output reporter]
  (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
    (.collect output (Text. word) (IntWritable. 1))))

(defn reducer-reduce [this key values output reporter]
  (.collect output key
            (reduce + (map #(.get %) (iterator-seq values)))))

(defn tool-run [this args]
  (doto (JobConf. (.getConf this) (.getClass this))
    (.setJobName "wordcount1")
    (.setOutputKeyClass Text)
    (.setOutputValueClass IntWritable)
    (.setMapperClass (Class/forName "wordcount1_mapper"))
    (.setReducerClass (Class/forName "wordcount1_reducer"))
    (.setInputFormat TextInputFormat)
    (.setOutputFormat TextOutputFormat)
    (FileInputFormat/setInputPaths (first args))
    (FileOutputFormat/setOutputPath (Path. (second args)))
    (JobClient/runJob))
  0)
