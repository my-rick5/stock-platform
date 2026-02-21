pipeline {
    agent any

    stages {
        stage('Static Analysis') {
            steps {
                echo 'Checking code style...'
                // Run flake8 inside the airflow-webserver container
                sh "docker compose run --rm --entrypoint /bin/bash airflow-webserver -c 'pip install flake8 && flake8 dags/'"
            }
        }

        stage('dbt Validation') {
            steps {
                echo 'Validating QuestDB Schema and Data Models...'
                // We use the 'dbt' service defined in your docker-compose
                sh "docker compose run --rm dbt debug"
                sh "docker compose run --rm dbt test"
            }
        }

        stage('Airflow Integrity') {
            steps {
                echo 'Verifying DAG import integrity...'
                // Reuse the same logic we used manually earlier
                sh "docker compose run --rm --entrypoint pytest airflow-webserver tests/test_dag_integrity.py"
            }
        }

        stage('Python Quality Gate') {
            steps {
                echo 'Running logic tests...'
                // Run pytest inside a fresh container and output the XML for Jenkins to read
                sh "docker compose run --rm --entrypoint /bin/bash airflow-webserver -c 'pip install pytest-mock && pytest tests/ -v --junitxml=results.xml'"
            }
            post {
                always {
                    // Pull the result file back from the workspace
                    junit 'results.xml'
                }
            }
        }

        stage('Deploy to Production') {
            when { branch 'main' }
            steps {
                echo 'Triggering containerized deployment...'
                sh 'chmod +x deploy.sh && ./deploy.sh'
            }
        }
    }
}