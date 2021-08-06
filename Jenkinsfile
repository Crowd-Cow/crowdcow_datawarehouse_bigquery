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
                database: QA_ANALYTICS
                role: SYSADMIN
                warehouse: TRANSFORMING
                schema: STAGING
                client_session_keep_alive: False
              prod:
                type: snowflake
                threads: 8
                account: lna65058.us-east-1
                user: $SNOWFLAKE_DATAWAREHOUSE_USER
                password: $SNOWFLAKE_DATAWAREHOUSE_PASSWORD
                database: ANALYTICS
                role: SYSADMIN
                warehouse: TRANSFORMING
                schema: STAGING
                client_session_keep_alive: False
            target: prod
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
        --env SNOWFLAKE_DATAWAREHOUSE_USER=$SNOWFLAKE_DATAWAREHOUSE_USER \
        --env SNOWFLAKE_DATAWAREHOUSE_PASSWORD=$SNOWFLAKE_DATAWAREHOUSE_PASSWORD  \
        crowdcow_datawarehouse_dbt_run \
        ./jenkins_bin/jenkins_run.sh
        '''
      }
    }
  }

  post {
    always {
      slackSend channel: '#jenkins-alerts', message: "Snowflake Data Warehouse ${currentBuild.displayName}: ${currentBuild.result}"
    }
  }
}
