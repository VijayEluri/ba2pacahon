sudo update-java-alternatives -s java-6-sun

#!/bin/sh
#sudo update-java-alternatives -s java-6-sun

rm convert.log

mdir=`pwd`

#cd ~/work/pacahon-tests/tests--json-ld--py
#pwd
#./pacahon_tester.py test001
#./pacahon_tester.py test002
#./pacahon_tester.py test003
#./pacahon_tester.py test004
#./pacahon_tester.py test005
#./pacahon_tester.py test006
#./pacahon_tester.py test007
#./pacahon_tester.py test008
#./pacahon_tester.py test009
#cd $mdir
java -d64 -XX:-UseGCOverheadLimit -Xms256m -Xmx1424m -cp ./target/ba2pacahon.jar -Djava.library.path=/usr/local/lib org.gost19.ba2pacahon.Fetcher>>convert.log

#>convert.log
