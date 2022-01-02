export PROJECT_ID=$(gcloud config list --format "value(core.project)")
export MY_REGION=us-central1

function start-iot-json-generator() {
  python cloudiot_mqtt_example_json.py \
     --project_id="$PROJECT_ID" \
     --cloud_region=$MY_REGION \
     --registry_id=iotlab-registry \
     --device_id=swe590-sensor-"$1" \
     --private_key_file=rsa_private.pem \
     --message_type=event \
     --algorithm=RS256
}

function start-iot-generator() {
  python cloudiot_mqtt_example.py \
     --project_id="$PROJECT_ID" \
     --cloud_region=$MY_REGION \
     --registry_id=iotlab-registry \
     --device_id=swe590-sensor-"$1" \
     --private_key_file=rsa_private.pem \
     --message_type=event \
     --algorithm=RS256
}

