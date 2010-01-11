;; wordcount3 -- example for use with clojure-hadoop.job
;;
;; This example wordcount program is very different from the first
;; two.  As you can see, it defines only two functions, doesn't import
;; anything, and doesn't generate any classes.
;;
;; This example is designed to be run with the clojure-hadoop.job
;; library, which allows you to run a MapReduce job that can be
;; configured to use any Clojure functions as the mapper and reducer.
;;
;; After compiling (see README.txt), run the example like this
;; (all on one line):
;;
;;   java -cp examples.jar clojure_hadoop.job \
;;        -input README.txt \
;;        -output out3 \
;;        -map clojure-hadoop.examples.wordcount3/my-map \
;;        -map-reader clojure-hadoop.wrap/int-string-map-reader \
;;        -reduce clojure-hadoop.examples.wordcount3/my-reduce \
;;        -input-format text
;;
;; The output is a Hadoop SequenceFile.  You can view the output
;; with (all one line):
;;
;;   java -cp examples.jar org.apache.hadoop.fs.FsShell \
;;        -text out3/part-00000 
;;
;; clojure_hadoop.job (note the underscore instead of a dash, because
;; we are calling it as a Java class) provides classes for Tool,
;; Mapper, and Reducer, which are dynamically configured on the command
;; line.
;;
;; The argument to -map is a namespace-qualified Clojure symbol.  It
;; names the function that will be used as a mapper.  We need to
;; specify the -map-reader function as well because we are not using
;; the default reader (which read pr'd Clojure data structures).
;;
;; The argument to -reduce is also a namespace-qualified Clojure
;; symbol.
;;
;; We also have to specify the input and output paths, and specify the
;; non-default input-format as 'text', because README.txt is a text
;; file.
;;
;; Run clojure_hadoop.job without any arguments for a brief summary of
;; the options.  See src/clojure_hadoop/job.clj and
;; src/clojure_hadoop/config.clj for more configuration options.
  

(ns clojure-hadoop.examples.wordcount3
  (:import (java.util StringTokenizer)))

(defn my-map [key value]
  (map (fn [token] [token 1])
       (enumeration-seq (StringTokenizer. value))))

(defn my-reduce [key values-fn]
  [[key (reduce + (values-fn))]])

