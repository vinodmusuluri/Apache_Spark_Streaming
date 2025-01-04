pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        S3_BUCKET = 'vinod123-test'
        EMR_CLUSTER_ID = 'j-vinod-test'
        TIMESTAMP = sh(script: 'date +%Y%m%d_%H%M%S', returnStdout: true).trim()
    }
    
    stages {
        stage('Upload to S3') {
            steps {
                script {
                    env.S3_FILE_PATH = "s3://${S3_BUCKET}/main_${TIMESTAMP}.py"
                    
                    withCredentials([
                        string(credentialsId: "AWS_ACCESS_KEY_ID", variable: 'AWS_ACCESS_KEY_ID'),
                        string(credentialsId: "AWS_SECRET_ACCESS_KEY", variable: 'AWS_SECRET_ACCESS_KEY')
                    ]) {
                        // Upload with timestamp to ensure latest version
                        sh """
                            aws s3 cp src/main.py ${env.S3_FILE_PATH} \
                                --region ${AWS_REGION}
                        """
                    }
                }
            }
        }
        
        stage('Run EMR Step') {
            steps {
                script {
                    withCredentials([
                        string(credentialsId: "AWS_ACCESS_KEY_ID", variable: 'AWS_ACCESS_KEY_ID'),
                        string(credentialsId: "AWS_SECRET_ACCESS_KEY", variable: 'AWS_SECRET_ACCESS_KEY')
                    ]) {
                        
                        sh """
                            aws emr add-steps \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --region ${AWS_REGION} \
                                --steps Type=Spark,Name=SparkJob_${TIMESTAMP},\
                                ActionOnFailure=CONTINUE,\
                                Args=[--deploy-mode,cluster,${env.S3_FILE_PATH}]
                                
                            # Wait for step to complete
                            aws emr wait step-complete \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --step-id \$(aws emr list-steps \
                                    --cluster-id ${EMR_CLUSTER_ID} \
                                    --region ${AWS_REGION} \
                                    --query 'Steps[0].Id' \
                                    --output text)
                        """
                    }
                }
            }
        }
    }
    
    post {
        success {
            echo "Successfully uploaded Python file to S3 and executed EMR step"
        }
        failure {
            echo "Failed to complete pipeline"
        }
    }
}
