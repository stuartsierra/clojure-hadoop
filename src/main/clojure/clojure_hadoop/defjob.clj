(ns clojure-hadoop.defjob
  (:require [clojure-hadoop.job :as job]))

(defn full-name
  "Returns the fully-qualified name for a symbol s, either a class or
  a var, resolved in the current namespace."
  [s]
  (if-let [v (resolve s)]
    (cond (var? v) (let [m (meta v)]
                     (str (ns-name (:ns m)) \/
                          (name (:name m))))
          (class? v) (.getName v))
    (throw (Exception. (str "Symbol not found: " s)))))

(defmacro defjob
  "Defines a job function. Options are the same those in
  clojure-hadoop.config.

  A job function may be given as the -job argument to
  clojure-hadoop.job to run a job."
  [name & options]
  (let [opts (apply hash-map options)
        args (reduce (fn [r [k v]]
                       (conj r (str \- (name k))
                             (cond (string? v) v
                                   (symbol? v) (full-name v)
                                   (keyword? v) (name v)
                                   :else (throw (Exception. "defjob arguments must be strings, symbols, or keywords")))))
                     [] (dissoc opts :class-name))])
  `(defn ~name [] ~args))
