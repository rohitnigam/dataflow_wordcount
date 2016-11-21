1. Make sure that java and mvn is installed in your computer and resides in the PATH and JAVA_HOME is set.
Once the project is cloned in the computer  git clone https://github.com/rohitnigam/dataflow_wordcount.git
cd dataflow_wordcount
2. mvn clean install
3. Go to your google project console and go to storage . Create two buckets one is a temporary staging location which dataflow programs requires and the
 other one is where the output would go.
 for example i created 2 buckets
   gs://iotlabtmpbucket
    and
  gs://wordcountbucket

  to run this program using GCP
  run this command :--
4. mvn exec:java -Dexec.mainClass=com.slb.WordCount -Dexec.args="--project=<YOUR-PROJECT-NAME> --stagingLocation=gs://iotlabtmpbucket --runner=BlockingDataflowPipelineRunner --output=gs://wordcountbucket/output --inputFile=gs://dataflow-samples/shakespeare/kinglear.txt"
