(ns clojure-hadoop.config
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-fs)
(imp/import-mapred)
(imp/import-mapred-lib)

(defn- as-str [s]
  (cond (keyword? s) (name s)
        (class? s) (.getName s)
        (fn? s) (throw (Exception. "Cannot use function as value; use a symbol."))
        :else (str s)))

(defmulti conf (fn [jobconf key value] key))

(defmethod conf :input [jobconf key value]
  (FileInputFormat/setInputPaths jobconf (as-str value)))

(defmethod conf :output [jobconf key value]
  (FileOutputFormat/setOutputPath jobconf (Path. (as-str value))))

(defmethod conf :replace [jobconf key value]
  (when (= (as-str value) "true")
    (.set jobconf "clojure-hadoop.job.replace" "true")))

(defmethod conf :map [jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "identity" value)
      (.setMapperClass jobconf IdentityMapper)

      (.contains value "/")
      (.set jobconf "clojure-hadoop.job.map" value)

      :else
      (.setMapperClass jobconf (Class/forName value)))))

(defmethod conf :reduce [jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "identity" value)
      (.setReducerClass jobconf IdentityReducer)

      (= "none" value)
      (.setNumReduceTasks jobconf 0)

      (.contains value "/")
      (.set jobconf "clojure-hadoop.job.reduce" value)

      :else
      (.setReducerClass jobconf (Class/forName value)))))

(defmethod conf :map-reader [jobconf key value]
  (.set jobconf "clojure-hadoop.job.map.reader" (as-str value)))

(defmethod conf :map-writer [jobconf key value]
  (.set jobconf "clojure-hadoop.job.map.writer" (as-str value)))

(defmethod conf :reduce-reader [jobconf key value]
  (.set jobconf "clojure-hadoop.job.reduce.reader" (as-str value)))

(defmethod conf :reduce-writer [jobconf key value]
  (.set jobconf "clojure-hadoop.job.reduce.writer" (as-str value)))

(defmethod conf :inputformat [jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "text" value)
      (.setInputFormat jobconf TextInputFormat)

      (= "kvtext" value)
      (.setInputFormat jobconf KeyValueTextInputFormat)

      (= "seq" value)
      (.setInputFormat jobconf SequenceFileInputFormat)

      :else
      (.setInputFormat jobconf (Class/forName value)))))

(defmethod conf :outputformat [jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "text" value)
      (.setOutputFormat jobconf TextOutputFormat)

      (= "seq" value)
      (.setOutputFormat jobconf SequenceFileOutputFormat)

      :else
      (.setOutputFormat jobconf (Class/forName value)))))

(defn parse-command-line-args [jobconf args]
  (when (empty? args)
    (throw (Exception. "Required options are -input, -output, -map, -reduce.")))
  (when-not (even? (count args))
    (throw (Exception. "Number of options must be even.")))
  (doseq [[k v] (partition 2 args)]
    (conf jobconf (keyword (subs k 1)) v)))

(defn parse-function-args [jobconf args]
  (when (empty? args)
    (throw (Exception. "Required options are :input, :output, :map, :reduce.")))
  (when-not (even? (count args))
    (throw (Exception. "Number of options must be even.")))
  (doseq [[k v] (partition 2 args)]
    (conf jobconf k v)))

(defn print-usage []
  (println "Usage: java -cp [jars...] clojure_hadoop.job [options...]
Required options are:
 -input     comma-separated input paths
 -output    output path
 -map       mapper function, as namespace/name or class name
 -reduce    reducer function, as namespace/name or class name

Mapper or reducer function may also be \"identity\".
Reducer function may also be \"none\".

Other available options are:
 -map-reader    Mapper reader function, as namespace/name
 -map-writer    Mapper writer function, as namespace/name
 -reduce-reader Reducer reader function, as namespace/name
 -reduce-writer Reducer writer function, as namespace/name
 -name          job name
 -replace       if \"true\", deletes output dir before start
"))

