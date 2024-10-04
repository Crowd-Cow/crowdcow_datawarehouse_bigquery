stage('Setup dbt profile') {
  steps {
    withCredentials([file(credentialsId: 'BigQueryServiceAccountKeyFile', variable: 'SERVICE_ACCOUNT_KEY')]) {

      // Create the .dbt directory and adjust permissions
      sh '''
        echo "Current user: $(whoami)"
        echo "Workspace directory permissions:"
        ls -ld $WORKSPACE
        mkdir -p $WORKSPACE/.dbt
        chmod u+w $WORKSPACE/.dbt
        echo "Permissions of .dbt directory:"
        ls -ld $WORKSPACE/.dbt
      '''

      // Verify service account key file
      sh '''
        if [ -f "$SERVICE_ACCOUNT_KEY" ]; then
          echo "Service account key file exists at $SERVICE_ACCOUNT_KEY"
        else
          echo "Service account key file not found at $SERVICE_ACCOUNT_KEY"
          exit 1
        fi
      '''

      // Copy the service account key into the .dbt directory
      sh '''
        cp "$SERVICE_ACCOUNT_KEY" $WORKSPACE/.dbt/bigquery_service_account_key.json
        chmod 600 $WORKSPACE/.dbt/bigquery_service_account_key.json
      '''

      // Create profiles.yml with BigQuery configuration
      sh """
        cat > $WORKSPACE/profiles.yml <<EOL
cc_datawarehouse:
  outputs:
    qa:
      type: bigquery
      method: service-account
      project: your-bigquery-project-id
      dataset: your_dataset_name
      threads: 16
      keyfile: /root/.dbt/bigquery_service_account_key.json
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 1
      maximum_bytes_billed: 100000000000
  target: qa
EOL
      """

      // Create Dockerfile to include profiles.yml and credentials
      sh """
        cat > $WORKSPACE/Dockerfile.crowdcow_datawarehouse <<EOL
FROM crowdcow_datawarehouse
COPY profiles.yml /root/.dbt/profiles.yml
COPY .dbt /root/.dbt/
EOL
      """

      // Build the final Docker image for the dbt run
      sh "docker build -f $WORKSPACE/Dockerfile.crowdcow_datawarehouse -t crowdcow_datawarehouse_dbt_run ."
    }
  }
}