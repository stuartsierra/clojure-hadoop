(ns clojure-hadoop.load)

(defn load-name
  "Loads and returns the value of a namespace-qualified string naming
  a symbol.  If the namespace is not currently loaded it will be
  require'd."
  [#^String s]
  (let [[ns-name fn-name] (.split s "/")]
    (when-not (find-ns (symbol ns-name))
      (require (symbol ns-name)))
    (assert (find-ns (symbol ns-name)))
    (deref (resolve (symbol ns-name fn-name)))))

