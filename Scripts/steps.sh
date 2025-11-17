aws emr add-steps \
    --cluster-id "j-88GPUVTMC4EC" \
    --steps '[{"Args":["bash", "-c", "mkdir -p /tmp/deps && pip3 install --target=/tmp/deps python-dateutil requests && export PYTHONPATH=/tmp/deps:$PYTHONPATH && aws s3 cp s3://nruizudatalake/scripts/ingesta_api.py /tmp/ingesta_api.py && python3 /tmp/ingesta_api.py"],"Type":"CUSTOM_JAR", "ActionOnFailure":"CONTINUE", "Jar":"command-runner.jar", "Name":"Ingesta API"}]'

aws emr add-steps \
    --cluster-id "j-88GPUVTMC4EC" \
    --steps '[{"Args":["bash", "-c", "mkdir -p /tmp/deps && pip3 install --target=/tmp/deps python-dateutil pymysql && export PYTHONPATH=/tmp/deps:$PYTHONPATH && aws s3 cp s3://nruizudatalake/scripts/exportar_rds.py /tmp/exportar_rds.py && python3 /tmp/exportar_rds.py"],"Type":"CUSTOM_JAR", "ActionOnFailure":"CONTINUE", "Jar":"command-runner.jar", "Name":"Exportar Datos RDS a S3"}]'