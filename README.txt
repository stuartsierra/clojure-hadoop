clojure-hadoop

An library to assist in writing Hadoop MapReduce jobs in Clojure.

by Stuart Sierra
http://stuartsierra.com/

For stable releases, see
http://stuartsierra.com/software/clojure-hadoop

For more information
on Clojure, http://clojure.org/
on Hadoop, http://hadoop.apache.org/

Also see my presentation about this library at
http://vimeo.com/7669741


Copyright (c) Stuart Sierra, 2009. All rights reserved.  The use and
distribution terms for this software are covered by the Eclipse Public
License 1.0 (http://opensource.org/licenses/eclipse-1.0.php) which can
be found in the file LICENSE.html at the root of this distribution.
By using this software in any fashion, you are agreeing to be bound by
the terms of this license.  You must not remove this notice, or any
other, from this software.



DEPENDENCIES

This library requires Java 6 JDK, http://java.sun.com/

Building from source requires Apache Maven 2, http://maven.apache.org/



BUILDING

If you downloaded the library distribution as a .zip or .tar file,
everything is pre-built and there is nothing you need to do.

If you downloaded the sources from Git, then you need to run the build
with Maven. In the top-level directory of this project, run:

    mvn assembly:assembly

This compiles and builds the JAR files.

You can find these files in the "target" directory (replace ${VERSION}
with the current version number of this library):

    clojure-hadoop-${VERSION}-examples.jar :

        This JAR contains all dependencies, including all of Hadoop
        0.18.3.  You can use this JAR to run the example MapReduce
        jobs from the command line.  This file is ONLY for running the
        examples.


    clojure-hadoop-${VERSION}-job.jar :

        This JAR contains the clojure-hadoop libraries and Clojure
        1.0.  It is suitable for inclusion in the "lib" directory of a
        JAR file submitted as a Hadoop job.


    clojure-hadoop-${VERSION}.jar :

        This JAR contains ONLY the clojure-hadoop libraries.  It can
        be placed in the "lib" directory of a JAR file submitted as a
        Hadoop job; that JAR must also include the Clojure 1.0 JAR.



RUNNING THE EXAMPLES

After building, copy the file from

    target/clojure-hadoop-${VERSION}-examples.jar

to something short, like "examples.jar".  Each of the *.clj files in
the src/examples directory contains instructions for running that
example.



USING THE LIBRARY IN HADOOP

After building, include the "clojure-hadoop-${VERSION}-job.jar" file
in the lib/ directory of the JAR you submit as your Hadoop job.



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
    ...
    <repositories>
      ...
      <!-- For released versions: -->
      <repository>
        <id>stuartsierra-releases</id>
        <name>Stuart Sierra's personal Maven 2 release repository</name>
        <url>http://stuartsierra.com/maven2</url>
      </repository>

      <!-- For SNAPSHOT versions: -->
      <repository>
        <id>stuartsierra-snapshots</id>
        <name>Stuart Sierra's personal Maven 2 SNAPSHOT repository</name>
        <url>http://stuartsierra.com/m2snapshots</url>
      </repository>
      ...
    </repositories>



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
