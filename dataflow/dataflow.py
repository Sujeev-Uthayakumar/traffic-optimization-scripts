import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
import csv
from io import StringIO
import psycopg2
import json
from google.cloud import storage

class FetchAndProcessFileJSON(beam.DoFn):
    def process(self, element):
        # Assuming element is a JSON byte string with the file name under "file_name" key
        message = json.loads(element.decode('utf-8'))
        file_name = message['name']
        bucket_name = "highd-dataset-final"
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        csv_string = blob.download_as_text()
        reader = csv.reader(csv_string.splitlines()[1:])
        for row in reader:
            processed_row = [
                int(row[0]),
                int(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                float(row[6]),
                float(row[7]),
                float(row[8]),
                float(row[9]),
                float(row[10]),
                float(row[11]),
                float(row[12]),
                float(row[13]),
                float(row[14]),
                float(row[15]),
                int(row[16]),
                int(row[17]),
                int(row[18]),
                int(row[19]),
                int(row[20]),
                int(row[21]),
                int(row[22]),
                int(row[23]),
            ]
            yield processed_row

class InsertIntoCloudSQL(beam.DoFn):
    def start_bundle(self):
        # Initialize database connection at the start of a bundle
        # Adjust with your Cloud SQL instance connection details
        self.connection = psycopg2.connect(
            dbname='', 
            user='', 
            password='', 
            host='', # Instead of the public IP
            port=''
        )
        self.cursor = self.connection.cursor()

    def process(self, element):
        # Assuming 'element' is the data to insert
        # Adjust the SQL statement as needed for your schema
        insert_statement = """INSERT INTO tracks (
                                frame, 
                                id, 
                                x, 
                                y, 
                                width, 
                                height, 
                                xVelocity, 
                                yVelocity, 
                                xAcceleration, 
                                yAcceleration, 
                                frontSightDistance, 
                                backSightDistance, 
                                dhw, 
                                thw, 
                                ttc, 
                                precedingXVelocity, 
                                precedingId, 
                                followingId, 
                                leftPrecedingId, 
                                leftAlongsideId, 
                                rightPrecedingId, 
                                rightAlongsideId, 
                                rightFollowingId, 
                                laneId
                                ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )"""
        element_tuple = tuple(element)
        print(element_tuple)

        self.cursor.execute(insert_statement, element_tuple)
        self.connection.commit()

    def finish_bundle(self):
        # Clean up: close the connection at the end of a bundle
        self.cursor.close()
        self.connection.close()

def run():
    pipeline_options = PipelineOptions(flags=[])
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    print("Streaming mode:", standard_options.streaming)
    
    with beam.Pipeline(options=pipeline_options) as p:
        file_contents = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/cloud-final-418702/topics/highd-topic')
            | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))
            | 'FetchAndProcessFileJSON' >> beam.ParDo(FetchAndProcessFileJSON())
        )
        
        file_contents | 'InsertIntoCloudSQL' >> beam.ParDo(InsertIntoCloudSQL())

if __name__ == "__main__":
    run()
