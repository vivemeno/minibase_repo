# modify the paths appropriately
JDKPATH = /usr/lib/jvm/default-java
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test project

Phase1:Phase1.java
	$(JAVAC) Phase1.java
phase1: Phase1
    $(JAVA) project.Phase1
