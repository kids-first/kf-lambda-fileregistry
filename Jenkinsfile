#!groovy
properties([
    pipelineTriggers([[$class:"GitHubPushTrigger"]])
])
pipeline {
  agent { label 'docker-slave' }
  stages{
    stage('Get Code') {
      steps {
          deleteDir()
          checkout ([
              $class: 'GitSCM',
              branches: scm.branches,
              doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
              extensions: [[$class: 'CloneOption', noTags: false, shallow: false, depth: 0, reference: '']],
              userRemoteConfigs: scm.userRemoteConfigs,
           ])
           script {
               tag=sh(returnStdout: true, script: "git tag -l --points-at HEAD").trim()
               env.tag = tag
             }
      }
    }
    stage('GetOpsScripts') {
      steps {
        slackSend (color: '#ddaa00', message: ":construction_worker: GETTING SCRIPTS:")
        sh '''
        git clone git@github.com:kids-first/kf-lambda-fileregistry-config.git
        '''
      }
    }
    stage('Test') {
     steps {
       slackSend (color: '#ddaa00', message: ":construction_worker: TESTING STARTED: (${env.BUILD_URL})")
       sh '''
       kf-lambda-fileregistry-config/aws-lambda/ci-scripts/test_stage/test.sh
       '''
       slackSend (color: '#41aa58', message: ":white_check_mark: TESTING COMPLETED: (${env.BUILD_URL})")
     }
     post {
       failure {
         slackSend (color: '#ff0000', message: ":frowning: Test Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
       }
     }
    }
    stage('Deploy Dev') {
      when {
        expression {
          return env.BRANCH_NAME != 'master';
        }
      }
      steps {
        slackSend (color: '#005e99', message: ":deploying_dev: DEPLOYING TO DEVELOPMENT: (${env.BUILD_URL})")
        sh '''
        kf-lambda-fileregistry-config/aws-lambda/ci-scripts/deploy_stage/deploy.sh dev
        '''
        slackSend (color: '#41aa58', message: ":white_check_mark: DEPLOYED TO DEVELOPMENT: (${env.BUILD_URL})")
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: Test Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Deploy QA') {
      when {
       expression {
           return env.BRANCH_NAME == 'master';
       }
     }
     steps {
       slackSend (color: '#005e99', message: ":deploying_qa: DEPLOYING TO QA: (${env.BUILD_URL})")
       sh '''
       kf-lambda-fileregistry-config/aws-lambda/ci-scripts/deploy_stage/deploy.sh qa
       '''
       slackSend (color: '#41aa58', message: ":white_check_mark: DEPLOYED TO QA: (${env.BUILD_URL})")
     }
    }
    stage("Promotion fileregistry-lambda to PRD") {
      when {
             expression {
               return env.BRANCH_NAME == 'master';
             }
             expression {
               return tag != '';
             }
           }
      steps {
             script {
                     env.DEPLOY_TO_PRD = input message: 'User input required',
                                     submitter: 'lubneuskia,heatha',
                                     parameters: [choice(name: 'fileregistry-lambda: Deploy to PRD Environment', choices: 'no\nyes', description: 'Choose "yes" if you want to deploy the PRD server')]
             }
     }
    }
    stage('Deploy PRD') {
      when {
       environment name: 'DEPLOY_TO_PRD', value: 'yes'
       expression {
           return env.BRANCH_NAME == 'master';
       }
       expression {
         return tag != '';
       }
     }
     steps {
       slackSend (color: '#005e99', message: ":deploying_prd: DEPLOYING TO PRD: (${env.BUILD_URL})")
       sh '''
       kf-lambda-fileregistry-config/aws-lambda/ci-scripts/deploy_stage/deploy.sh prd
       '''
       slackSend (color: '#41aa58', message: ":white_check_mark: DEPLOYED TO PRD: (${env.BUILD_URL})")
     }
    }
    stage("Rollback fileregistry-lambda?") {
      when {
             expression {
               return env.BRANCH_NAME == 'master';
             }
             expression {
               return tag != '';
             }
           }
      steps {
             script {
                     env.ROLL_BACK = input message: 'User input required',
                                     submitter: 'lubneuskia,heatha',
                                     parameters: [choice(name: 'fileregistry-lambda: Deploy to PRD Environment', choices: 'no\nyes', description: 'Choose "yes" if you want to deploy the PRD server')]
             }
     }
    }
  }
}
