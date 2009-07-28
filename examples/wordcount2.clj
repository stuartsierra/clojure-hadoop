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
    (FileInputFormat/setInputPaths (apply str (first args)))
    (FileOutputFormat/setOutputPath (Path. (second args)))
    (JobClient/runJob))
  0)
