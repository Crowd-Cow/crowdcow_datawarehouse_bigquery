pipeline {
    agent any

    stages {
        stage('Checkout and Build Image') {
            steps {
                checkout scm

                
                sh "docker build -t crowdcow_datawarehouse_bigquery ."
            }
        }

        stage('Setup dbt profile and Credentials') {
            steps {
                // Write the profiles.yml for BigQuery
                sh '''
                cat > profiles.yml <<EOL
                cc_bigquery_datawarehouse:
                  target: qa
                  outputs:
                    prod:
                      type: bigquery
                      method: service-account  
                      project: panoply-0ef-a098d410468d
                      dataset: ANALYTICS
                      threads: 8
                      keyfile: /root/.dbt/bigquery_service_account_key.json
                      OPTIONAL_CONFIG: VALUE
                    qa:
                      type: bigquery
                      method: service-account  
                      project: panoply-0ef-a098d410468d
                      dataset: qa
                      threads: 8
                      keyfile: /root/.dbt/bigquery_service_account_key.json
                      timeout_seconds: 300
                      OPTIONAL_CONFIG: VALUE
                
                EOL
                '''

                // Create the .dbt directory
                sh "mkdir -p .dbt"

                // Use 'withCredentials' to handle the service account key securely
                withCredentials([file(credentialsId: 'BigQueryServiceAccountKeyFile', variable: 'SERVICE_ACCOUNT_KEY')]) {
                    // Copy the key file into the .dbt directory
                    sh 'cp "$SERVICE_ACCOUNT_KEY" .dbt/bigquery_service_account_key.json'
                }

                // Create a Dockerfile that copies profiles.yml and credentials
                sh '''
                cat > Dockerfile.crowdcow_datawarehouse_bigquery <<EOL
                FROM crowdcow_datawarehouse_bigquery
                COPY profiles.yml /root/.dbt/profiles.yml
                COPY .dbt /root/.dbt/
                EOL
                '''

                // Build the new Docker image
                sh "docker build -f Dockerfile.crowdcow_datawarehouse_bigquery -t crowdcow_datawarehouse_bigquery_dbt_run ."
            }
        }

        stage('RUN') {
            steps {
                // Run the container with the necessary environment variable
                sh '''
                docker run \
                --rm \
                -e GOOGLE_APPLICATION_CREDENTIALS=/root/.dbt/bigquery_service_account_key.json \
                crowdcow_datawarehouse_dbt_run \
                ./jenkins_bin/jenkins_run.sh
                '''
            }
        }
    }

    
}