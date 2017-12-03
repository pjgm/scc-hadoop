How to run hadoop job:

* Set JAVA_HOME and HADOOP_CLASSPATH
    * export JAVA_HOME=/usr/java/default
    * export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
* run hadoop com.sun.tools.javac.Main WordCount.java
* run hadoop jar src/main/java/wc.jar WordCount <input> <output> on project root
    * input must contain files to process
    * output is the name of the output folder and should not already exist