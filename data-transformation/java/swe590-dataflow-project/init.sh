export PROJECT_ID=my-project-1509787322529
export REGION=us-central1
export TOPIC_ID=swe590-pubsub-topic
export BUCKET_NAME=swe590-bucket
export GOOGLE_APPLICATION_CREDENTIALS=../../../../my-project-1509787322529-c23342119a92.json
export BIGQUERY_DATASET_NAME=swe590_term_project_dataset
export BIGQUERY_TABLE_NAME=swe590_term_project_table

function deploy-dataflow-pipeline() {
  mvn clean compile exec:java \
  -Dexec.mainClass=com.omeregeozkaya.boun.PubSubToBigQueryIotDataPipeline \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=$PROJECT_ID \
    --region=$REGION \
    --inputTopic=projects/$PROJECT_ID/topics/$TOPIC_ID \
    --output=gs://$BUCKET_NAME/temp/dataflow/helloworld \
    --runner=DataflowRunner \
    --windowSize=10 \
    --workerMachineType=n1-standard-1 \
    --maxNumWorkers=5 \
    --numWorkers=1 \
    --autoscalingAlgorithm=THROUGHPUT_BASED \
    --bigQueryDatasetName=$BIGQUERY_DATASET_NAME \
    --bigQueryTableName=$BIGQUERY_TABLE_NAME \
  "
}

function test-dataflow-pipeline() {
  mvn clean compile exec:java \
  -Dexec.mainClass=com.omeregeozkaya.boun.PubSubToBigQueryIotDataPipeline \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=$PROJECT_ID \
    --region=$REGION \
    --inputTopic=projects/$PROJECT_ID/topics/$TOPIC_ID \
    --output=gs://$BUCKET_NAME/temp/dataflow/helloworld \
    --runner=DirectRunner \
    --windowSize=10 \
    --workerMachineType=n1-standard-1 \
    --maxNumWorkers=5 \
    --numWorkers=1 \
    --autoscalingAlgorithm=THROUGHPUT_BASED \
    --bigQueryDatasetName=$BIGQUERY_DATASET_NAME \
    --bigQueryTableName=$BIGQUERY_TABLE_NAME \
  "
}

echo 'Enter "deploy-dataflow-pipeline" to deploy dataflow pipeline.'
