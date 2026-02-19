pipeline {
    agent any

    options {
        // Prevents logs from filling up and stops hung processes
        timeout(time: 1, unit: 'HOURS')
        buildDiscarder(logRotator(numToKeepStr: '5'))
    }

    stages {
        stage('Checkout') {
            steps {
                // Jenkins pulls the code from your GitHub repo automatically
                echo 'Checking out code from GitHub...'
                checkout scm
            }
        }

        stage('Environment Setup') {
            steps {
                echo 'Verifying Docker environment...'
                sh 'docker version'
                sh 'docker compose version'
            }
        }

        stage('Down / Cleanup') {
            steps {
                echo 'Stopping existing containers to ensure a clean state...'
                // || true prevents the build from failing if containers aren't running
                sh 'docker compose down || true'
            }
        }

        stage('Build & Deploy') {
            steps {
                echo 'Building and starting the stock-platform stack...'
                // --build ensures your latest Dockerfile/dbt changes are included
                sh 'docker compose up -d --build'
            }
        }

        stage('Post-Deployment Health Check') {
            steps {
                echo 'Waiting for QuestDB to initialize...'
                // Simple retry loop to check if QuestDB web console is up
                script {
                    sh 'sleep 10'
                    sh 'curl -f http://localhost:9000 || exit 1'
                }
            }
        }
    }

    post {
        success {
            echo 'SUCCESS: BTC Refresh Stack is live at http://localhost:8080'
        }
        failure {
            echo 'FAILURE: Check Docker logs or Jenkins Console Output.'
        }
    }
}
