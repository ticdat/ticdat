pipeline {
    agent {
        docker {
            image 'opexanalytics/r-py-gurobi:3.8.1'
            args '-v /var/run/docker.sock:/var/run/docker.sock -v /usr/bin/docker:/usr/bin/docker'
        }
    }

    options {
      buildDiscarder(logRotator(numToKeepStr: '5'))
    }    

    stages {
        stage('Checkout'){
            steps {
                checkout scm
            }
        }

        stage('Build'){
            steps {
                echo 'Building docker image'
                withDockerRegistry([ credentialsId: "DockerHubLogin", url: "" ]) {
                    sh 'docker build --no-cache --force-rm --rm=true -f Dockerfile -t opexanalytics/ticdat:dev .'
                }                
            }
        }

        stage('Push'){
            steps {
                echo 'Push to docker registries'
                withDockerRegistry([ credentialsId: "DockerHubLogin", url: "" ]) {
                    sh 'docker push opexanalytics/ticdat:dev'
                }                
            }
        }

	stage('Run'){
            steps {
                echo 'Run tests'
                withDockerRegistry([ credentialsId: "DockerHubLogin", url: "" ]) {
                    sh 'docker run opexanalytics/ticdat:dev'
                }
            }
        }
    }
}
