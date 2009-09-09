(ns clojure-hadoop.config
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-fs)
(imp/import-mapred)
(imp/import-mapred-lib)

(defmulti conf (fn [jobconf key value] key))

(defmethod conf "-input" [jobconf key value]
  (FileInputFormat/setInputPaths jobconf value))

(defmethod conf "-output" [jobconf key value]
  (FileOutputFormat/setOutputPath jobconf (Path. value)))

(defmethod conf "-replace" [jobconf key value]
  (when (= value "true")
    (.set jobconf "clojure-hadoop.job.replace" "true")))

(defmethod conf "-map" [jobconf key value]
  (cond
   (= "identity" (.toLowerCase value))
   (.setMapperClass jobconf IdentityMapper)

   (.contains value "/")
   (.set jobconf "clojure-hadoop.job.map" value)

   :else
   (.setMapperClass jobconf (Class/forName value))))

(defmethod conf "-reduce" [jobconf key value]
  (cond
   (= "identity" (.toLowerCase value))
   (.setReducerClass jobconf IdentityReducer)

   (= "none" (.toLowerCase value))
   (.setNumReduceTasks jobconf 0)

   (.contains value "/")
   (.set jobconf "clojure-hadoop.job.reduce" value)

   :else
   (.setReducerClass jobconf (Class/forName value))))

(defmethod conf "-inputformat" [jobconf key value]
  (prn key value)
  (cond
   (= "text" (.toLowerCase value))
   (.setInputFormat jobconf TextInputFormat)

   (= "kvtext" (.toLowerCase value))
   (.setInputFormat jobconf KeyValueTextInputFormat)

   (= "seq" (.toLowerCase value))
   (.setInputFormat jobconf SequenceFileInputFormat)

   :else
   (.setInputFormat jobconf (Class/forName value))))

(defmethod conf "-outputformat" [jobconf key value]
  (cond
   (= "text" (.toLowerCase value))
   (.setOutputFormat jobconf TextOutputFormat)

   (= "seq" (.toLowerCase value))
   (.setOutputFormat jobconf SequenceFileOutputFormat)

   :else
   (.setOutputFormat jobconf (Class/forName value))))

(defn parse-args [jobconf args]
  (when (empty? args)
    (throw (Exception. "Required options are -input, -output, -map, -reduce.")))
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
 -name          job name
 -replace       if \"true\", deletes output dir before start
"))

