    agent any
    environment {
        AWS_ACCESS_KEY_ID = ''
        AWS_SECRET_ACCESS_KEY = ''
        AWS_REGION = 'us-east-1'
    }
    stages {
        stage(‘artifacts copy to S3) {
            steps {
                script {
                    // Confirm AWS credentials are set correctly
                    sh """
                    export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                    export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                   


                    Aws s3 cp *****************
                    """
                }
            }
        } 
    }
}

