;; wordcount4 -- example precompiled job
;;
;; This example wordcount program is similar to wordcount3, but it is
;; packaged in a generated class to simplify running from the command
;; line.
;;
;; The namespace declares :gen-class, which generates a class with a
;; static main method.
;;
;; The -main method definition just calls the main method of
;; clojure-hadoop.job, passing in some pre-configured arguments.
;;
;; After compiling (see README.txt), run the example like this
;; (all on one line):
;;
;;   java -cp examples.jar \
;;        clojure_hadoop.examples.wordcount4 \
;;        README.txt out4
;;
;; The output is a Hadoop SequenceFile.  You can view the output
;; with (all one line):
;;
;;   java -cp examples.jar org.apache.hadoop.fs.FsShell \
;;        -text out4/part-00000 
  

(ns clojure-hadoop.examples.wordcount4
  (:gen-class)
  (:require [clojure-hadoop.job :as job])
  (:import (java.util StringTokenizer)))

(defn my-map [key value]
  (map (fn [token] [token 1])
       (enumeration-seq (StringTokenizer. value))))

(defn my-reduce [key values]
  [[key (reduce + values)]])

(defn job []
  {:map my-map
   :map-reader wrap/int-string-map-reader
   :reduce my-reduce
   :inputformat :text})

