python dataflow.py \ 
--runner DataflowRunner \
--project cloud-final-418702 \
--region northamerica-northeast2 \
--temp_location gs://highd-dataset-final/temp \
--staging_location gs://highd-dataset-final/staging \
--streaming \
--experiment use_unsupported_python_version \
--requirements_file ./requirements.txt 