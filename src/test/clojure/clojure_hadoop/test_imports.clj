(ns clojure-hadoop.test-imports
  (:require [clojure-hadoop.imports :as imp])
  (:use clojure.test))

(deftest test-imports
  (imp/import-io)
  (imp/import-io-compress)
  (imp/import-fs)
  (imp/import-mapred)
  (imp/import-mapred-lib)
  (imp/import-util))
