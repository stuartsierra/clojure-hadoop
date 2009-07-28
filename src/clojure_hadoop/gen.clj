(ns clojure-hadoop.gen)

(defmacro gen-job-classes []
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

(defn gen-main-method []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    (intern *ns* 'tool-main
            (fn [& args]
              (System/exit
               (org.apache.hadoop.util.ToolRunner/run 
                (new org.apache.hadoop.conf.Configuration)
                (. (Class/forName the-name) newInstance)
                (into-array String args)))))))
