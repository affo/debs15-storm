FROM affo/storm:0.9.5

ADD build/libs/debs15-storm.jar $STORM_HOME/lib/
ADD build/libs/debs15-storm.jar /
# show contents of jar files
RUN jar tf debs15-storm.jar

ENTRYPOINT [ "storm", "jar", "debs15-storm.jar" ]
