pipeline {
  agent any

  environment {
    SNOWFLAKE_DATAWAREHOUSE_USER = credentials('SnowflakeDatawarehouseUser')
    SNOWFLAKE_DATAWAREHOUSE_PASSWORD = credentials('SnowflakeDatawarehousePassword')
  }

  stages {
    stage('Checkout and Build Image') {
      steps {
        checkout scm

        sh "docker build -t crowdcow_datawarehouse ."
      }
    }

    stage('Setup dbt profile') {
      steps {
        sh """
          cat > profiles.yml <<EOL
          cc_datawarehouse:
            outputs:
              qa:
                type: snowflake
                threads: 8
                account: lna65058.us-east-1
                user: $SNOWFLAKE_DATAWAREHOUSE_USER
                password: $SNOWFLAKE_DATAWAREHOUSE_PASSWORD
                database: ANALYTICS_QA
                role: TRANSFORMER
                warehouse: TRANSFORMING
                schema: STAGING
                client_session_keep_alive: False
            target: qa
        """

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
        sh '''
        docker run \
        --rm \
        crowdcow_datawarehouse_dbt_run \
        ./jenkins_bin/jenkins_run.sh
        '''
      }
    }
  }

  post {
    failure {
      slackSend channel: '#jenkins-alerts', message: ":red_circle: ${currentBuild.projectName} ${currentBuild.displayName}: ${currentBuild.result}"
    }
  }
}
