export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export TOPIC_ID=swe590-pubsub-topic
export BUCKET_NAME=swe590-bucket
export GOOGLE_APPLICATION_CREDENTIALS=../../../../my-project-1509787322529-c23342119a92.json

function deploy-dataflow-pipeline() {
  mvn clean compile exec:java \
  -Dexec.mainClass=com.omeregeozkaya.boun.StarterPipeline \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=$PROJECT_ID \
    --region=$REGION \
    --inputTopic=projects/$PROJECT_ID/topics/$TOPIC_ID \
    --output=gs://$BUCKET_NAME/temp/dataflow/helloworld \
    --runner=DataflowRunner \
    --windowSize=10 \
    --workerMachineType=n1-standard-1 \
    --maxNumWorkers=2 \
    --numWorkers=1 \
    --autoscalingAlgorithm=THROUGHPUT_BASED
  "
}

echo 'Enter "deploy-dataflow-pipeline" to deploy dataflow pipeline.'
