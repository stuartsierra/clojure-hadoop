(ns clojure-hadoop.gen
;;#^{:doc "Class-generation helpers for writing Hadoop jobs in Clojure."}
)

(defmacro gen-job-classes
  "Creates gen-class forms for Hadoop job classes from the current
  namespace. Now you only need to write three functions:

  (defn mapper-map [this key value output reporter] ...)

  (defn reducer-reduce [this key values output reporter] ...)

  (defn tool-run [& args] ...)

  The first two functions are the standard map/reduce functions in any
  Hadoop job.

  The third function, tool-run, will be called by the Hadoop framework
  to start your job, with the arguments from the command line.  It
  should set up the JobConf object and call JobClient/runJob, then
  return zero on success.

  You must also call gen-main-method to create the main method.

  After compiling your namespace, you can run it as a Hadoop job using
  the standard Hadoop command-line tools."
  []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    `(do 
       (gen-class
        :name ~the-name
        :extends "org.apache.hadoop.conf.Configured"
        :implements ["org.apache.hadoop.util.Tool"]
        :prefix "tool-"
        :main true)
       (gen-class
        :name ~(str the-name "_mapper")
        :extends "org.apache.hadoop.mapred.MapReduceBase"
        :implements ["org.apache.hadoop.mapred.Mapper"]
        :prefix "mapper-")
       (gen-class
        :name ~(str the-name "_reducer")
        :extends "org.apache.hadoop.mapred.MapReduceBase"
        :implements ["org.apache.hadoop.mapred.Reducer"]
        :prefix "reducer-"))))

(defn gen-main-method
  "Adds a standard main method, named tool-main, to the current
  namespace."
  []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    (intern *ns* 'tool-main
            (fn [& args]
              (System/exit
               (org.apache.hadoop.util.ToolRunner/run 
                (new org.apache.hadoop.conf.Configuration)
                (. (Class/forName the-name) newInstance)
                (into-array String args)))))))
