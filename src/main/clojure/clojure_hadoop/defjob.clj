(ns clojure-hadoop.defjob
  (:require [clojure-hadoop.job :as job]))

(defn- full-name
  "Returns the fully-qualified name for a symbol s, either a class or
  a var, resolved in the current namespace."
  [s]
  (if-let [v (resolve s)]
    (cond (var? v) (let [m (meta v)]
                     (str (ns-name (:ns m)) \/
                          (name (:name m))))
          (class? v) (.getName #^Class v))
    (throw (Exception. (str "Symbol not found: " s)))))

(defmacro defjob
  "Defines a job function. Options are the same those in
  clojure-hadoop.config.

  A job function may be given as the -job argument to
  clojure_hadoop.job to run a job."
  [sym & options]
  (let [args (reduce (fn [m [k v]]
                       (assoc m k
                              (cond (keyword? v) (name v)
                                    (string? v) v
                                    (symbol? v) (full-name v)
                                    (instance? Boolean v) (str v)
                                    :else (throw (Exception. "defjob arguments must be strings, symbols, or keywords")))))
                     {} (apply hash-map options))]
    `(defn ~sym [] ~args)))
