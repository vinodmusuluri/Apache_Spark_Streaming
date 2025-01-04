pipeline {
    agent any
    
    // Use Jenkins credentials instead of hardcoding in environment
    environment {
        AWS_REGION = 'us-east-1'
        // Define other environment variables if needed
    }
    
    parameters {
        string(name: 'S3_BUCKET', defaultValue: '', description: 'S3 bucket name')
        string(name: 'S3_PATH', defaultValue: '', description: 'Path within S3 bucket')
        string(name: 'FILE_PATTERN', defaultValue: '*', description: 'File pattern to upload')
    }
    
    stages {
        stage('Validate Parameters') {
            steps {
                script {
                    if (!params.S3_BUCKET) {
                        error "S3_BUCKET parameter is required"
                    }
                }
            }
        }
        
        stage('AWS Authentication') {
            steps {
                script {
                    // Use Jenkins credentials binding
                    withCredentials([[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: 'aws-credentials',  // Create this credential ID in Jenkins
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                    ]]) {
                        // Verify AWS authentication
                        sh 'aws sts get-caller-identity'
                    }
                }
            }
        }
        
        stage('Copy to S3') {
            steps {
                script {
                    withCredentials([[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: 'aws-credentials',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                    ]]) {
                        try {
                            // Construct S3 path
                            def s3Path = "s3://${params.S3_BUCKET}/${params.S3_PATH}".replaceAll('/+', '/')
                            
                            // Upload files to S3
                            sh """
                                aws s3 cp ${params.FILE_PATTERN} ${s3Path} \
                                    --region ${AWS_REGION} \
                                    --recursive
                            """
                            
                            // Verify upload
                            sh """
                                aws s3 ls ${s3Path} \
                                    --region ${AWS_REGION}
                            """
                        } catch (Exception e) {
                            currentBuild.result = 'FAILURE'
                            error "Failed to upload to S3: ${e.getMessage()}"
                        }
                    }
                }
            }
        }
    }
    
    post {
        success {
            echo "Successfully uploaded artifacts to S3"
        }
        failure {
            echo "Failed to upload artifacts to S3"
        }
        always {
            // Clean up workspace if needed
            cleanWs()
        }
    }
}
