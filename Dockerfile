FROM brscrt/java-8
MAINTAINER Baris Cirit "brscrt@gmail.com"
ADD VertxConsumerGr-latest.tar /
ENTRYPOINT ["/VertxConsumerGr-latest/bin/VertxConsumerGr"]
