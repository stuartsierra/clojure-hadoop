(ns clojure-hadoop.config
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-fs)
(imp/import-mapred)

(defmulti conf (fn [jobconf key value] key))

(defmethod conf "-input" [jobconf key value]
  (FileInputFormat/setInputPaths jobconf value))

(defmethod conf "-output" [jobconf key value]
  (FileOutputFormat/setOutputPath jobconf value))

(defmethod conf "-replace" [jobconf key value]
  (when (= value "true")
    (.set jobconf "clojure-hadoop.config.replace" "true")))

(defmethod conf "-map" [jobconf key value]
  (.set jobconf "clojure-hadoop.job.map" value))

(defmethod conf "-reduce" [jobconf key value]
  (.set jobconf "clojure-hadoop.job.reduce" value))

(defn parse-args [jobconf args]
  (when (empty? args)
    (println "Required options are:
 -input     comma-separated input paths
 -output    output path
 -map       mapper function, as namespace/name
 -reduce    reducer function, as namespace/name

Other available options are:
 -name          job name
 -replace       if followed by \"true\", overwrites output
")
    (System/exit 0))
  (when-not (even? (count args))
    (throw (Exception. "Arguments must be even; run without args for help.")))
  (doseq [[k v] (partition 2 args)]
    (conf jobconf k v)))

(defn handle-replace-option [jobconf]
  (when (= "true" (.get jobconf "clojure-hadoop.config.replace"))
    nil))
