(ns #^{:doc "Map/Reduce wrappers that set up common input/output
  conversions for Clojure jobs."}
  clojure-hadoop.wrap
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-mapred)

(declare *reporter*)

(defn wrap-map
  "Returns a function implementing the Mapper.map interface.

  The returned function uses read-string to read in Clojure data
  structures from the key and value (which must be of type
  org.apache.hadoop.io.Text).  Then it calls (f key value).

  f must return a *sequence* of *pairs* like 
    [[key1 value1] [key2 value2] ...]

  The pairs returned by f will be serialized to strings with pr-str,
  then sent to the Hadoop OutputCollector as Text objects.

  When f is called, *reporter* is bound to the Hadoop Reporter."
  [f]
  (fn [this wkey wvalue output reporter]
    (binding [*reporter* reporter]
      (doseq [[key value] (f (read-string (.toString wkey))
                             (read-string (.toString wvalue)))]
        (binding [*print-dup* true]
          (.collect output (Text. (pr-str key)) (Text. (pr-str value))))))))

(defn wrap-reduce
  "Returns a function implementing the Reducer.reduce interface.

  The returned function uses read-string to read in Clojure data
  structures from the key and values (which must be of type
  org.apache.hadoop.io.Text).  Then it calls (f key values).

  f must return a *sequence* of *pairs* like 
    [[key1 value1] [key2 value2] ...]

  The pairs returned by f will be serialized to strings with pr-str,
  then sent to the Hadoop OutputCollector as Text objects.

  When f is called, *reporter* is bound to the Hadoop Reporter."
  [f]
  (fn [this wkey wvalues output reporter]
    (binding [*reporter* reporter]
      (doseq [[key value] (f (read-string (.toString wkey))
                             (map #(read-string (.toString %)) (iterator-seq wvalues)))]
        (binding [*print-dup* true]
          (.collect output (Text. (pr-str key)) (Text. (pr-str value))))))))
