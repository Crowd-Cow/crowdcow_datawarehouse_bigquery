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
            cat > profiles.yml <<EOL
            cc_bigquery_datawarehouse:
              outputs:
                prod:
                  type: bigquery
                  method: service-account  
                  project: panoply-0ef-a098d410468d
                  dataset: ANALYTICS
                  threads: 8
                  keyfile: /usr/src/app/service_account.json
                  OPTIONAL_CONFIG: VALUE
                qa:
                  type: bigquery
                  method: service-account  
                  project: panoply-0ef-a098d410468d
                  dataset: qa
                  threads: 8
                  keyfile: /usr/src/app/service_account.json
                  OPTIONAL_CONFIG: VALUE
              target: qa
          """
        }
      }
    }

    stage('Build Run Image') {
      steps {
        sh """
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
          sh 'echo "Jenkins workspace: $(pwd)"'
          sh 'ls -l $BIGQUERY_SERVICE_ACCOUNT_KEY'  // Verify the credential file
          sh 'cp $BIGQUERY_SERVICE_ACCOUNT_KEY ./service_account.json'
          sh 'ls -l ./service_account.json'  // Verify the copied file
          sh 'file ./service_account.json'   // Check if it's a file

          // Adjust permissions if necessary
          sh 'chmod 644 ./service_account.json'

          sh """
          docker run \
          --rm \
          -v ${WORKSPACE}/service_account.json:/usr/src/app/service_account.json \
          crowdcow_datawarehouse_dbt_run \
          ./jenkins_bin/jenkins_run.sh
          """

          sh 'rm ./service_account.json'
        }
      }
    }
  }

  post {
    cleanup {
      sh 'rm -f ./service_account.json profiles.yml Dockerfile.crowdcow_datawarehouse'
    }
  }
}