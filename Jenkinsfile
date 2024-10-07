pipeline {
    agent any

    environment {
        // Access the BigQuery service account key as an environment variable
        BIGQUERY_SERVICE_ACCOUNT_KEY = credentials('BigQueryServiceAccountKey')
    }

    stages {
        stage('Checkout and Build Image') {
            steps {
                checkout scm

                // Build the base Docker image
                sh "docker build -t crowdcow_datawarehouse ."
            }
        }

        stage('Setup dbt profile') {
            steps {
                // Create profiles.yml with BigQuery configuration
                sh '''
                cat > profiles.yml <<EOL
cc_datawarehouse:
  outputs:
    qa:
      type: bigquery
      method: service-account-json
      project: panoply-0ef-a098d410468d
      dataset: qa
      threads: 16
      keyfile_json: "{{ env_var('BIGQUERY_SERVICE_ACCOUNT_KEY') }}"
      timeout_seconds: 300
      location: US
      priority: interactive
    prod:
      type: bigquery
      method: service-account-json
      project: panoply-0ef-a098d410468d
      dataset: ANALYTICS
      threads: 16
      keyfile_json: "{{ env_var('BIGQUERY_SERVICE_ACCOUNT_KEY') }}"
      timeout_seconds: 300
      location: US
      priority: interactive
  target: qa
EOL
                '''

                // Create Dockerfile to include profiles.yml
                sh '''
                cat > Dockerfile.crowdcow_datawarehouse <<EOL
FROM crowdcow_datawarehouse
COPY profiles.yml /root/.dbt/profiles.yml
EOL
                '''

                // Build the final Docker image for the dbt run
                sh "docker build -f Dockerfile.crowdcow_datawarehouse -t crowdcow_datawarehouse_dbt_run ."
            }
        }

        stage('RUN') {
            steps {
                // Run the dbt commands inside the Docker container
                sh '''
                docker run \
                --rm \
                -e BIGQUERY_SERVICE_ACCOUNT_KEY="$BIGQUERY_SERVICE_ACCOUNT_KEY" \
                crowdcow_datawarehouse_dbt_run \
                ./jenkins_bin/jenkins_run.sh
                '''
            }
        }
    }

}