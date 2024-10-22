pipeline {
  agent any

  stages {
    stage('Checkout and Build Base Image') {
      steps {
        checkout scm

        sh "docker build -t crowdcow_datawarehouse ."
      }
    }

    stage('Setup dbt Profile') {
      steps {
        withCredentials([file(credentialsId: 'BigQueryServiceAccountKeyFile', variable: 'BIGQUERY_SERVICE_ACCOUNT_KEY')]) {
          sh """
            # Remove existing profiles.yml if it exists
            rm -f profiles.yml

            cat > profiles.yml <<EOL
            cc_bigquery_datawarehouse:
              outputs:
                prod:
                  type: bigquery
                  method: service-account  
                  project: panoply-0ef-a098d410468d
                  dataset: analytics
                  threads: 8
                  keyfile: /tmp/service-account-key.json
                  OPTIONAL_CONFIG: VALUE
                qa:
                  type: bigquery
                  method: service-account  
                  project: panoply-0ef-a098d410468d
                  dataset: qa
                  threads: 8
                  keyfile: /tmp/service-account-key.json
                  OPTIONAL_CONFIG: VALUE
              target: qa
          """
        }
      }
    }

    stage('Build Run Image') {
      steps {
        sh """
           # Remove existing Dockerfile if it exists
          rm -f Dockerfile.crowdcow_datawarehouse
          cat > Dockerfile.crowdcow_datawarehouse <<EOL
          FROM crowdcow_datawarehouse
          COPY profiles.yml /root/.dbt/profiles.yml
        """

        sh "docker build -f Dockerfile.crowdcow_datawarehouse -t crowdcow_datawarehouse_dbt_run ."
      }
    }

    stage('RUN') {
      steps {
        withCredentials([file(credentialsId: 'BigQueryServiceAccountKeyFile', variable: 'BIGQUERY_SERVICE_ACCOUNT_KEY')]) {
          script {
            // Remove existing container if it exists
            sh '''
            docker rm -f dbt_run_container || true
            '''
            // Start the container in detached mode
            sh '''
            docker run -d --name dbt_run_container \
              crowdcow_datawarehouse_dbt_run \
              sleep infinity
            '''

            // Copy the service account key into the container
            sh '''
            docker cp $BIGQUERY_SERVICE_ACCOUNT_KEY dbt_run_container:/tmp/service-account-key.json
            '''

            // Execute your script inside the container
            sh '''
            docker exec dbt_run_container /bin/bash -c "./jenkins_bin/jenkins_run.sh"
            '''

            // Stop and remove the container
            sh '''
            docker stop dbt_run_container
            docker rm dbt_run_container
            '''
          }
        }
      }
    }
  }
  post {
    always {
      // Clean up the workspace and remove any residual files or containers
      sh '''
      docker rm -f dbt_run_container || true
      rm -f profiles.yml Dockerfile.crowdcow_datawarehouse
      '''
    }
  // failure {
  //    slackSend channel: '#jenkins-alerts', message: ":red_circle: ${currentBuild.projectName} ${currentBuild.displayName}: ${currentBuild.result}"
  // }
  }
}