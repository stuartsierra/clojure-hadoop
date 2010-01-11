;; wordcount5 -- example customized defjob
;;
;; This example wordcount program uses defjob like wordcount4, but it
;; includes some more configuration options that make it more
;; efficient.  
;;
;; In the default configuration (wordcount4), everything is passed to
;; Hadoop as a Text and converted by the Clojure reader and printer.
;; By adding configuration options, this example works more closely
;; with Hadoop types like LongWritable.  In order to do that it must
;; define custom reader and writer functions, and specify the output
;; key/value types in the defjob configuration.
;;
;; After compiling (see README.txt), run the example like this
;; (all on one line):
;;
;;   java -cp examples.jar clojure_hadoop.job \
;;        -job clojure-hadoop.examples.wordcount5/job \
;;        -input README.txt -output out5
;;
;; The output is plain text, written to out5/part-00000
;;
;; Notice that the strings in the output are not quoted.  In effect,
;; we have come full circle to wordcount1, while maintaining the
;; separation between the mapper/reducer functions and the
;; reader/writer functions.
  

(ns clojure-hadoop.examples.wordcount5
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob]
            [clojure-hadoop.imports :as imp])
  (:import (java.util StringTokenizer)))

(imp/import-io)  ;; for Text, LongWritable
(imp/import-mapred)  ;; for OutputCollector

(defn my-map [key value]
  (map (fn [token] [token 1])
       (enumeration-seq (StringTokenizer. value))))

(defn my-reduce [key values-fn]
  [[key (reduce + (values-fn))]])

(defn string-long-writer [#^OutputCollector output
                          #^String key value]
  (.collect output (Text. key) (LongWritable. value)))

(defn string-long-reduce-reader [#^Text key wvalues]
  [(.toString key)
   (fn [] (map (fn [#^LongWritable v] (.get v))
               (iterator-seq wvalues)))])

(defjob/defjob job
  :map my-map
  :map-reader wrap/int-string-map-reader
  :map-writer string-long-writer
  :reduce my-reduce
  :reduce-reader string-long-reduce-reader
  :reduce-writer string-long-writer
  :output-key Text
  :output-value LongWritable
  :input-format :text
  :output-format :text
  :compress-output false)

