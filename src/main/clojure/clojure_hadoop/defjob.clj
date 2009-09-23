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
  "Generates a job class for AOT-compilation.  The class has a main
  function which calls clojure-hadoop.job/tool-main.

  Options are the same those in clojure-hadoop.job/config.

  One additional option, :class-name, specifies the name of the
  generated class.  If not given, defaults to the name of the current
  namespace.

  You may use have multiple defjobs in a single namespace, as long as
  they have different :class-names."
  [& options]
  (let [opts (apply hash-map options)
        class-name (or (str (:class-name opts))
                       (.replace (name (ns-name *ns*)) \- \_))
        prefix (str (gensym "defjob"))
        args (reduce (fn [r [k v]]
                       (conj r (str \- (name k))
                             (cond (string? v) v
                                   (symbol? v) (full-name v)
                                   (keyword? v) (name v)
                                   :else (throw (Exception. "defjob arguments must be strings, symbols, or keywords")))))
                     [] (dissoc opts :class-name))])
  `(do (gen-class :name ~class-name :prefix ~prefix :main true)
       (intern *ns* (symbol (str prefix "-main"))
               (fn [& args#]
                 (job/tool-main (concat ~args args#))))))
