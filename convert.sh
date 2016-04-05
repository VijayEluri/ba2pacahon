#!/bin/sh
#sudo update-java-alternatives -s java-6-sun

rm convert.log

java -d64 -cp ./target/ba2veda.jar -Djava.library.path=/usr/local/lib org.gost19.ba2veda.Fetcher
#>>convert.log
