(ns wordcount3
  (:require [clojure-hadoop.wrap :as wrap])
  (:import (java.util StringTokenizer)))

(defn my-map [this key value output reporter]
  (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
    (.collect output (Text. (pr-str word)) (Text. (pr-str 1)))))

(def my-reduce
     (wrap/wrap-reduce
      (fn [key values]
        [[key (reduce + values)]])))

