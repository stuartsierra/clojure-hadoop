;; wordcount3 -- example for use with clojure-hadoop.job
;;
;; This example wordcount program is very different from the first two.
;; As you can see, it defines only one function, doesn't import
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
;;        -map clojure_hadoop.examples.wordcount2_mapper \
;;        -reduce clojure-hadoop.examples.wordcount3/my-reduce \
;;        -inputformat text
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
;; The argument to -map is a class name, in this case from wordcount2,
;; so that class gets used as the Mapper implementation.
;;
;; The argument to -reduce is a namespace-qualified Clojure symbol.  It
;; names the function defined below.  When the Reducer runs, it will
;; use that function (wrapped with clojure-hadoop.wrap/wrap-reduce) as
;; the Reducer.reduce implementation.
;;
;; We also have to specify the input and output paths, and specify the
;; non-default inputformat as 'text', because README.txt is a text
;; file.
;;
;; Run clojure_hadoop.job without any arguments for a brief summary of
;; the options.  See src/clojure_hadoop/job.clj and
;; src/clojure_hadoop/config.clj for more configuration options.
  

(ns clojure-hadoop.examples.wordcount3)

(defn my-reduce [key values]
  [[key (reduce + values)]])

