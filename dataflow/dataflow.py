import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

class ProcessData(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # Implement your processing logic here
        print(el)
        return [f"Processed: {element}"]

options = PipelineOptions(
    runner='DataflowRunner',
    project='your-gcp-project-id',
    region='your-region',
    temp_location='gs://your-bucket/temp',
    staging_location='gs://your-bucket/staging',
)

with beam.Pipeline(options=options) as pipeline:
    (pipeline
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/your-gcp-project-id/subscriptions/events-subscription')
     | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))  # 60-second fixed windows
     | 'ProcessData' >> beam.ParDo(ProcessData())
     | 'WriteToPubSub' >> beam.io.WriteToPubSub('gs://your-bucket/output')
    )
