FROM java:8
RUN mkdir /opt/storm
RUN curl -o - http://mirrors.muzzy.it/apache/storm/apache-storm-0.9.5/apache-storm-0.9.5.tar.gz | tar -zxf - --strip-components=1 -C /opt/storm
ENV STORM_HOME /opt/storm
RUN ln -s $STORM_HOME/bin/storm /usr/bin/storm

ENTRYPOINT [ "storm" ]
