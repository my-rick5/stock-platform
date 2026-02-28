pipeline {
    agent any

    environment {
        // Set paths to your local binaries if they aren't in Jenkins' default PATH
        SPARK_HOME = '/opt/homebrew/opt/apache-spark/libexec'
        PATH = "${SPARK_HOME}/bin:${PATH}"
    }

    stages {
        stage('Environment Check') {
            steps {
                echo '🔍 Verifying Native Infrastructure...'
                // Check if Kafka and QuestDB are actually running before we try to test
                sh 'nc -z localhost 9092 || (echo "Kafka is DOWN" && exit 1)'
                sh 'nc -z localhost 8812 || (echo "QuestDB is DOWN" && exit 1)'
            }
        }

        stage('Static Analysis') {
            steps {
                echo '💅 Checking code style (flake8)...'
                // Runs locally on your Mac
                sh 'pip install flake8 --quiet'
                sh 'flake8 scripts/*.py'
            }
        }

        stage('Unit Tests') {
            steps {
                echo '🧪 Running logic tests (pytest)...'
                // Test your ML logic and data transformations
                sh 'pip install pytest pytest-mock --quiet'
                sh 'pytest tests/ -v --junitxml=results.xml'
            }
            post {
                always {
                    junit 'results.xml'
                }
            }
        }

        stage('Deploy Stream') {
            when { 
                anyOf {
                    branch 'main'
                    branch 'master'
                    branch 'fix-jenkins-setup' // Added your current branch for testing
                }
            }
            steps {
                echo '🚀 Launching Bare Metal Stack...'
                // Using the root script we just moved
                sh 'chmod +x launch_stack.sh'
                sh './launch_stack.sh'
            }
        }
    }
    
    post {
        failure {
            echo '❌ Pipeline Failed. Checking logs...'
            // Optional: run your stop script if the deploy fails to clean up ghost processes
            sh './stop.sh || true' 
        }
    }
}