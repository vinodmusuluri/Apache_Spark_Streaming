pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        S3_BUCKET = 'vinod123-test'
        EMR_CLUSTER_ID = 'j-1OJRJ1LWIFDLA'
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
                        sh """
                            # Test S3 access first
                            echo "Testing S3 access..."
                            aws s3 ls ${S3_BUCKET} --region ${AWS_REGION}
                            
                            # Upload the file
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
                            # First verify cluster access
                            echo "Verifying EMR cluster access..."
                            aws emr describe-cluster --cluster-id ${EMR_CLUSTER_ID} --region ${AWS_REGION}
                            
                            # Add EMR step with direct credentials
                            aws emr add-steps \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --region ${AWS_REGION} \
                                --steps '[{
                                    "Type": "Spark",
                                    "Name": "SparkJob_${TIMESTAMP}",
                                    "ActionOnFailure": "CONTINUE",
                                    "Args": [
                                        "--deploy-mode",
                                        "cluster",
                                        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                                        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                                        "--conf", "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}",
                                        "--conf", "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}",
                                        "--conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true",
                                        "--conf", "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true",
                                        "--conf", "spark.hadoop.fs.s3a.endpoint=s3.${AWS_REGION}.amazonaws.com",
                                        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
                                        "--conf", "spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}",
                                        "--conf", "spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}",
                                        "--conf", "spark.executorEnv.PYTHONPATH=/mnt/var/lib/spark/python/lib/py-files",
                                        "--conf", "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                                        "${env.S3_FILE_PATH}"
                                    ]
                                }]'
                            
                            # Wait for step to complete
                            STEP_ID=\$(aws emr list-steps \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --region ${AWS_REGION} \
                                --query 'Steps[0].Id' \
                                --output text)
                                
                            echo "Waiting for step \$STEP_ID to complete..."
                            aws emr wait step-complete \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --step-id \$STEP_ID \
                                --region ${AWS_REGION}
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
            script {
                withCredentials([
                    string(credentialsId: "AWS_ACCESS_KEY_ID", variable: 'AWS_ACCESS_KEY_ID'),
                    string(credentialsId: "AWS_SECRET_ACCESS_KEY", variable: 'AWS_SECRET_ACCESS_KEY')
                ]) {
                    sh """
                        # Get the latest step ID
                        STEP_ID=\$(aws emr list-steps \
                            --cluster-id ${EMR_CLUSTER_ID} \
                            --region ${AWS_REGION} \
                            --query 'Steps[0].Id' \
                            --output text)
                        
                        echo "Fetching detailed error information..."
                        aws emr describe-step \
                            --cluster-id ${EMR_CLUSTER_ID} \
                            --step-id \$STEP_ID \
                            --region ${AWS_REGION}
                        
                        echo "Fetching cluster status..."
                        aws emr describe-cluster \
                            --cluster-id ${EMR_CLUSTER_ID} \
                            --region ${AWS_REGION}
                            
                        echo "Checking S3 access..."
                        aws s3 ls ${S3_BUCKET} --region ${AWS_REGION} || true
                    """
                }
            }
        }
    }
}
