export PROJECT_ID=$(gcloud config list --format "value(core.project)")
export MY_REGION=us-central1
export MY_REGISTRY=swe590-iot-registry
source venv/bin/activate

function start-iot-json-generator() {
  python cloudiot_mqtt_example_json.py \
     --project_id="$PROJECT_ID" \
     --cloud_region=$MY_REGION \
     --registry_id=$MY_REGISTRY \
     --device_id=swe590-sensor-"$1" \
     --private_key_file=rsa_private.pem \
     --message_type=event \
     --algorithm=RS256 \
     --delay_in_seconds="1" \
     --num_messages=100
}

function start-iot-generator() {
  python cloudiot_mqtt_example.py \
     --project_id="$PROJECT_ID" \
     --cloud_region=$MY_REGION \
     --registry_id=$MY_REGISTRY \
     --device_id=swe590-sensor-"$1" \
     --private_key_file=rsa_private.pem \
     --message_type=event \
     --algorithm=RS256
}

printf '\nUse "start-iot-json-generator" or "start-iot-generator" commands with argument "1" or "2" as the sensor id.'
printf '\nExample: "start-iot-json-generator 1"\n\n'
