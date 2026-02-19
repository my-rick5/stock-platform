pipeline {
    agent any

    environment {
        // Points to your QuestDB container inside the Docker network
        DBT_PROFILES_DIR = "${WORKSPACE}/analytics"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Analytics') {
            steps {
                script {
                    // Build the dbt image we just perfected
                    sh 'docker compose build dbt'
                }
            }
        }

        stage('Data Quality Enforcement') {
            steps {
                script {
                    try {
                        // Run the tests using the flags we discovered
                        sh 'docker compose run --rm dbt test --no-populate-cache'
                    } catch (Exception e) {
                        error "Data quality tests failed. Check QuestDB for anomalies."
                    }
                }
            }
        }
    }

    post {
        always {
            // Clean up stopped containers to keep the Jenkins runner clean
            sh 'docker compose down'
        }
        success {
            echo '✅ Data Quality Enforced: Bitcoin price data is valid!'
        }
        failure {
            echo '❌ CI Failed: The data in QuestDB does not meet quality standards.'
        }
    }
}