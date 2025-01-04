pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        S3_BUCKET = 'vinod123-test'
    }
    
    stages {
        stage('Upload to S3') {
            steps {
                script {
                    withCredentials([
                        string(credentialsId: "AWS_ACCESS_KEY_ID", variable: 'AWS_ACCESS_KEY_ID'),  // Access Key as secret text
                        string(credentialsId: "AWS_SECRET_ACCESS_KEY", variable: 'AWS_SECRET_ACCESS_KEY')  // Secret Key as secret text
                    ]) {
                        // Upload a single file to S3 using the AWS credentials provided
                        sh """
                            aws s3 cp src/main.py s3://${S3_BUCKET}/ \
                                --region ${AWS_REGION}
                        """
                    }
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
