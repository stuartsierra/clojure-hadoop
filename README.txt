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

This library requires:
1. Java 6 JDK, http://java.sun.com/
2. Apache Maven 2, http://maven.apache.org/



INSTALLING

In the top-level directory of this project, run:

    mvn install

This installs the clojure-hadoop JAR in your local Maven 2 repository.
Then run:

    mvn assembly:assembly

This builds alternate JAR files, with dependencies included, for
running the examples.  

You can find these files in the "target" directory (replace ${VERSION}
with the current version number of this library):

    clojure-hadoop-${VERSION}-examples.jar
        This JAR contains all dependencies, including all of Hadoop
        0.18.3.  You can use this JAR to run the examples MapReduce
        jobs from the command line.

    clojure-hadoop-${VERSION}-job.jar
        This JAR contains only this library and Clojure 1.0.  It is
        suitable for inclusion in the "lib" directory of a JAR file
        submitted as a Hadoop job.



RUNNING THE EXAMPLES

After running "mvn assembly:assembly", copy the file from

    target/clojure-hadoop-${VERSION}-examples.jar

to something short, like "examples.jar".  Each of the *.clj files in
the src/examples directory contains instructions for running that
example.



USING THE LIBRARY IN HADOOP

Run "mvn assembly:assembly" in this project, then include the
"clojure-hadoop-${VERSION}-job.jar" file in the lib/ directory of
the JAR you submit as your Hadoop job.



DEPENDING ON THE LIBRARY WITH MAVEN

You can depend on clojure-hadoop in your Maven 2 projects by adding
the following lines to your pom.xml:

    <dependencies>
      ...

      <dependency>
        <groupId>com.stuartsierra</groupId>
        <artifactId>clojure-hadoop</artifactId>
        <version>${VERSION}</version>
      </dependency>

      ...
    </dependencies>



USING THE LIBRARY

This library provides different layers of abstraction away from the
raw Hadoop API.

Layer 1: clojure-hadoop.imports

    Provides convenience functions for importing the many classes and
    interfaces in the Hadoop API.

Layer 2: clojure-hadoop.gen

    Provides gen-class macros to generate the multiple classes needed
    for a MapReduce job.  See the file "examples/wordcount1.clj" for a
    demonstration of these macros.

Layer 3: clojure-hadoop.wrap

    clojure-hadoop.wrap: provides wrapper functions that automatically
    convert between Hadoop Text objects and Clojure data structures.
    See the file "examples/wordcount2.clj" for a demonstration of
    these wrappers.

Layer 4: clojure-hadoop.job

    Provides a complete implementation of a Hadoop MapReduce job that
    can be dynamically configured to use any Clojure functions in the
    map and reduce phases.  See the file "examples/wordcount3.clj" for
    a demonstration of this usage.
