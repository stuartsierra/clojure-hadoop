;; wordcount4 -- example defjob
;;
;; This example wordcount program is similar to wordcount3, but it
;; includes a job definition function created with defjob.
;;
;; defjob parses its options to create a job configuration map
;; suitable for clojure-hadoop.config.
;;
;; defjob defines an ordinary function, with the given name ("job" in
;; this example), which returns the job configuration map.
;;
;; We can specify the job definition function on the command line to
;; clojure_hadoop.job, adding or overriding any additional arguments
;; at the command line.
;;
;; After compiling (see README.txt), run the example like this
;; (all on one line):
;;
;;   java -cp examples.jar clojure_hadoop.job \
;;        -job clojure-hadoop.examples.wordcount4/job \
;;        -input README.txt -output out4
;;
;; The output is a Hadoop SequenceFile.  You can view the output
;; with (all one line):
;;
;;   java -cp examples.jar org.apache.hadoop.fs.FsShell \
;;        -text out4/part-00000 
  

(ns clojure-hadoop.examples.wordcount4
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob])
  (:import (java.util StringTokenizer)))

(defn my-map [key value]
  (map (fn [token] [token 1])
       (enumeration-seq (StringTokenizer. value))))

(defn my-reduce [key values-fn]
  [[key (reduce + (values-fn))]])

(defjob/defjob job
  :map my-map
  :map-reader wrap/int-string-map-reader
  :reduce my-reduce
  :input-format :text)

