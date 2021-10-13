pipeline {
  agent {
    node {
      label 'AEP-wf-01'
    }

  }
  stages {
    stage('pull code') {
      steps {
        git(url: 'https://github.com/asd19951995/kvdk.git', branch: 'jenkins_test', credentialsId: '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')
      }
    }

  }
}