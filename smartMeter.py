import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1

project_id = "analog-premise-414918"
input_topic = "input_topic"
output_topic = "output_topic"

class ConvertUnits(beam.DoFn):
    def process(self, element):
        pressure_kpa = element['pressure']
        temperature_celsius = element['temperature']

        if pressure_kpa is None or temperature_celsius is None:
            return

        pressure_psi = pressure_kpa / 6.895
        temperature_fahrenheit = temperature_celsius * 1.8 + 32

        element['pressure'] = pressure_psi
        element['temperature'] = temperature_fahrenheit

        return [element]

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        readings = p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=f'projects/{project_id}/topics/{input_topic}')

        filtered_readings = readings | 'Filter' >> beam.Filter(lambda x: x['pressure'] is not None and x['temperature'] is not None)

        converted_readings = filtered_readings | 'Convert Units' >> beam.ParDo(ConvertUnits())

        converted_readings | 'Write to PubSub' >> beam.io.WriteToPubSub(topic=f'projects/{project_id}/topics/{output_topic}')

if __name__ == '__main__':
    run()