(ns wordcount3
  (:import (java.util StringTokenizer)))

(defn my-reduce [key values]
  [[key (reduce + values)]])

