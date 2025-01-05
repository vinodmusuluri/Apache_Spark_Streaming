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
                                --steps '[{
                                    "Type": "Spark",
                                    "Name": "SparkJob_${TIMESTAMP}",
                                    "ActionOnFailure": "CONTINUE",
                                    "Args": [
                                        "--master",
                                        "yarn",
                                        "--deploy-mode",
                                        "cluster",
                                        "--conf",
                                        "spark.sql.extensions=net.snowflake.spark.snowflake",
                                        "--conf",
                                        "spark.driver.extraPythonPath=/tmp",
                                        "--conf",
                                        "spark.executor.extraPythonPath=/tmp",
                                        "--conf",
                                        "spark.jars=s3://aws-glue-reltio-bucket/snowflake-jars/snowflake-jdbc-3.19.0.jar,s3://aws-glue-reltio-bucket/snowflake-jars/spark-snowflake_2.12-3.1.0.jar,s3://aws-glue-reltio-bucket/snowflake-jars/spark-avro_2.12-3.4.0.jar",
                                        "--conf",
                                        "spark.submit.pyFiles=s3://aws-glue-reltio-bucket/snowflake-jars/Apache_Spark_Streaming.zip",
                                        "--conf",
                                        "spark.yarn.appMasterEnv.PYTHONPATH=/tmp",
                                        "--conf",
                                        "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                                        "--archives",
                                        "s3://aws-glue-reltio-bucket/snowflake-jars/Apache_Spark_Streaming.zip#/tmp/Apache_Spark_Streaming",
                                        "--py-files",
                                        "s3://aws-glue-reltio-bucket/snowflake-jars/Apache_Spark_Streaming.zip",
                                        "${env.S3_FILE_PATH}"
                                    ]
                                }]'
                            
                            # Get the step ID
                            STEP_ID=\$(aws emr list-steps \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --region ${AWS_REGION} \
                                --query 'Steps[0].Id' \
                                --output text)
                            
                            echo "Waiting for step \$STEP_ID to complete..."
                            aws emr wait step-complete \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --step-id \$STEP_ID
                            
                            # Check the final status
                            STEP_STATE=\$(aws emr describe-step \
                                --cluster-id ${EMR_CLUSTER_ID} \
                                --step-id \$STEP_ID \
                                --query 'Step.Status.State' \
                                --output text)
                            
                            if [ "\$STEP_STATE" != "COMPLETED" ]; then
                                echo "Step failed. Getting error details..."
                                aws emr describe-step \
                                    --cluster-id ${EMR_CLUSTER_ID} \
                                    --step-id \$STEP_ID \
                                    --query 'Step.Status.FailureDetails.Message' \
                                    --output text
                                exit 1
                            fi
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
            echo "Pipeline failed. Check EMR step logs for details."
        }
    }
}
