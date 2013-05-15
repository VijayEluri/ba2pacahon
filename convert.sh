#!/bin/sh
#sudo update-java-alternatives -s java-6-sun

rm convert.log

java -d64 -cp ./target/ba2pacahon.jar org.gost19.ba2pacahon.Fetcher
#>>convert.log
