import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

class ProcessData(beam.DoFn):
    def process(self, element):
        # Assume element is a byte string; decode it, process it, and re-encode it
        # This example simply prefixes the string with "Processed: " for demonstration
        processed_element = f"Processed: {element.decode('utf-8')}".encode('utf-8')
        print(processed_element)
        return [processed_element]


options = PipelineOptions(
    runner='DataflowRunner',
    project='cloud-final-418702',
    region='northamerica-northeast2',
    temp_location='gs://highd-dataset-final/temp',
    staging_location='gs://highd-dataset-final/staging',
    streaming=True
)

# Define your pipeline
with beam.Pipeline(options=options) as p:
    (p 
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/cloud-final-418702/topics/highd-topic')
     | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))
     | 'ProcessData' >> beam.ParDo(ProcessData())  # Replace with your processing function
     | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic='projects/cloud-final-418702/topics/highd-processed-topic')
    )
