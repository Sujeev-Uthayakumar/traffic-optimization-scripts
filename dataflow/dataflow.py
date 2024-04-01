import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

class ProcessData(beam.DoFn):
    def process(self, element):
        # Implement your processing logic here
        return [f"Processed: {element}"]

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
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/cloud-final-418702/subscriptions/highd-subscription')
     | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))
     | 'ProcessData' >> beam.ParDo(ProcessData())  # Replace with your processing function
    )
