(ns clojure-hadoop.wrap
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-mapred)

(declare *reporter*)

(defn wrap-map [f]
  (fn [this wkey wvalue output reporter]
    (binding [*reporter* reporter]
      (doseq [[key value] (f (read-string (.toString wkey))
                             (read-string (.toString wvalue)))]
        (binding [*print-dup* true]
          (.collect output (Text. (pr-str key)) (Text. (pr-str value))))))))

(defn wrap-reduce [f]
  (fn [this wkey wvalues output reporter]
    (binding [*reporter* reporter]
      (doseq [[key value] (f (read-string (.toString wkey))
                             (map #(read-string (.toString %)) (iterator-seq wvalues)))]
        (binding [*print-dup* true]
          (.collect output (Text. (pr-str key)) (Text. (pr-str value))))))))
