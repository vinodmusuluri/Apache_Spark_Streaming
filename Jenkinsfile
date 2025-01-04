pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        S3_BUCKET = 'vinod123-test'
        AWS_ACCESS_KEY_ID = 'AKIAWGNYAX2ZZV7VXHZB'
        AWS_SECRET_ACCESS_KEY = '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'
    }
    
    stages {
        stage('Upload to S3') {
            steps {
                script {
                    // Upload a single file to S3
                    sh """
                        export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                        export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                        aws s3 cp src/main.py s3://${S3_BUCKET}/ \
                            --region ${AWS_REGION}
                    """
                }
            }
        }
    }
    
    post {
        success {
            echo "Successfully uploaded main.py to S3"
        }
        failure {
            echo "Failed to upload main.py to S3"
        }
    }
}
