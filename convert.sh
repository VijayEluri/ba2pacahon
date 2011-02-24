#!/bin/sh
rm convert.log

mdir=`pwd`

cd ~/work/pacahon-tests/tests--json-ld
pwd
./pacahon_tester.py test001
./pacahon_tester.py test002
./pacahon_tester.py test003
./pacahon_tester.py test004
./pacahon_tester.py test005
./pacahon_tester.py test006
./pacahon_tester.py test007
./pacahon_tester.py test008
./pacahon_tester.py test009
cd $mdir
java -cp ./target/ba2pacahon.jar -Djava.library.path=/usr/local/lib org.gost19.ba2pacahon.Fetcher>convert.log
#org.gost19.ba2pacahon.Fetcher doc>>export_document_type.log
