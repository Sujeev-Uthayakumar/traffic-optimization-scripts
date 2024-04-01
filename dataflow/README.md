enable dataflow api
need service accouts with compute engine service agent
pub sub admin

pip install pip --upgrade
pip install 'apache-beam[gcp]'

PROJECT=$(gcloud config list project --format "value(core.project)")
BUCKET=gs://$PROJECT-bucket

The design section requires the use of two topics, one subscriber, and the Dataflow job running in the background. The producer will take advantage of the topic used in lab named, lab1-topic that will then be used for input field in the Dataflow job. The Dataflow job will then output it to the smartMeter_output topic. There is a subscriber in place to listen to the smartMeter_output topic that will display the altered values, with the name smartMeter_output_subscription. The producer.py will create a topic with the smart meter reading. The Dataflow pipeline will process the smart meter data. The consumer will finally output the smart meter data with the data altered based off the formula in the Dataflow pipeline. The Dataflow command must ensure that the input and output flags take the appropriate topic names. The input should be the lab1-topic with the output being the smartMeter_output topic.
