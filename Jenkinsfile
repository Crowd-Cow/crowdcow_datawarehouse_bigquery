pipeline {
  agent any

  stages {
    stage('Checkout and Build Image') {
      steps {
        checkout scm

        sh "docker build -t crowdcow_datawarehouse_bigquery ."
      }
    }

    stage('Setup dbt profile') {
      steps {
        withCredentials([file(credentialsId: 'BigQueryServiceAccountKeyFile', variable: 'SERVICE_ACCOUNT_KEY')]) {

          // Create the .dbt directory in the workspace
          sh '''
            echo "Current user: $(whoami)"
            echo "Workspace directory permissions:"
            ls -ld $WORKSPACE
            mkdir -p $WORKSPACE/.dbt
            chmod u+w $WORKSPACE/.dbt
            echo "Permissions of .dbt directory:"
            ls -ld $WORKSPACE/.dbt
          '''

          // Copy the service account key into the .dbt directory
          sh 'cp "$SERVICE_ACCOUNT_KEY" $WORKSPACE/.dbt/bigquery_service_account_key.json'

          // Create profiles.yml with BigQuery configuration
          sh """
            cat > $WORKSPACE/profiles.yml <<EOL
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
          """

          // Create Dockerfile to include profiles.yml and credentials
          sh """
            cat > $WORKSPACE/Dockerfile.crowdcow_datawarehouse_bigquery <<EOL
FROM crowdcow_datawarehouse_bigquery
COPY profiles.yml /root/.dbt/profiles.yml
COPY .dbt /root/.dbt/
EOL
          """

          // Build the final Docker image for the dbt run
          sh "docker build -f Dockerfile.crowdcow_datawarehouse_bigquery -t crowdcow_datawarehouse_dbt_run ."
        }
      }
    }

    stage('RUN') {
      steps {
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

