clojure-hadoop

An library to assist in writing Hadoop MapReduce jobs in Clojure.

by Stuart Sierra
http://stuartsierra.com/

For more information
on Clojure, http://clojure.org/
on Hadoop, http://hadoop.apache.org/

Copyright (c) Stuart Sierra, 2009. All rights reserved.  The use and
distribution terms for this software are covered by the Eclipse Public
License 1.0 (http://opensource.org/licenses/eclipse-1.0.php) which can
be found in the file epl-v10.html at the root of this distribution.
By using this software in any fashion, you are agreeing to be bound by
the terms of this license.  You must not remove this notice, or any
other, from this software.



DEPENDENCIES

In order to compile and run this code, you will need the following JAR
files in your classpath:

 1. clojure.jar
 2. hadoop-0.18.3-core.jar
 3. Dependent JARs included with Hadoop (such as commons-logging)

This code was developed with Hadoop MapReduce 0.18.3, although it
should work with any later version.

You can download the Hadoop distribution by visiting this web page:
http://www.apache.org/dyn/closer.cgi/hadoop/core/hadoop-0.18.3/
and selecting a mirror close to you.



COMPILING (OPTIONAL)

1. Create a "lib" directory in the same directory as this file and
copy the JARs listed above into it.

2. Run "ant"



USING THE LIBRARY

This library includes the following 3 namespaces:

clojure-hadoop.imports: provides convenience functions for importing
the many classes and interfaces in the Hadoop API.

clojure-hadoop.gen: provides gen-class macros to generate the multiple
classes needed for a MapReduce job.  See the file
"examples/wordcount1.clj" for a demonstration of these macros.

clojure-hadoop.wrap: provides wrapper functions that automatically
convert between Hadoop Text objects and Clojure data structures.  See
the file "examples/wordcount2.clj" for a demonstration of these
wrappers.
