pipeline {
  agent {
    node {
      label 'agent01'
    }

  }
  stages {
    stage('source') {
      steps {
        git 'https://github.com/Merlinkim/gold_coin.git'
      }
    }

    stage('deploy') {
      steps {
        sh '''ssh -i ./key jenkins_key@64.343.366.353
cd airflow
cd dags
git clone git@ffffff.git'''
      }
    }

    stage('end') {
      steps {
        sh 'echo "done"'
      }
    }

  }
}