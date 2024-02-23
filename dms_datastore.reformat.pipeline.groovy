pipeline {
    agent any
    environment {
        //Location of the repository
        REPO='y:\\repo\\continuous'
        REPO_STAGING_REF='y:\\repo_staging\\continuous'
        REPO_STAGING='y:\\jenkins_repo_staging\\continuous'
        CONDA_BIN='d:\\ProgramData\\miniconda3\\condabin'
    }
    stages {
        stage('Mount Network Drive') {
            steps {
                script {
                    // Define the network path and drive letter
                    def networkPath = '\\\\cnrastore-bdo\\modeling_data'
                    def driveLetter = 'Y:'

                    // Execute the command and capture the output
                    def script = '''
                    @echo off
                    if exist Y: (
                        echo true
                    ) else (
                        echo false
                    )
                    '''
                    def cmdOutput = bat(script: script, returnStdout: true).trim()
                    echo "Command Output: '${cmdOutput}'"

                    // Determine if the drive is mounted
                    def isMounted = cmdOutput == 'true'

                    if (!isMounted) {
                        echo "Mounting ${driveLetter} to ${networkPath}"
                        bat "net use ${driveLetter} ${networkPath} /persistent:no"
                        echo "Mounted ${networkPath} as ${driveLetter}"
                    } else {
                        echo "${driveLetter} is already mounted."
                    }

                    bat 'dir Y:\\'
                }
            }
        }
        stage('ensure formatted dir') {
            steps {
                dir("${env.REPO_STAGING}/formatted"){
                    // write file with date created
                    script {
                        def now = new Date()
                        CREATE_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                    }
                    writeFile file:'created.txt', text:CREATE_TIME
                }
            }
        }
        stage('reformat') {
            steps{
                dir("${env.REPO_STAGING}"){
                    bat 'call %CONDA_BIN%\\conda activate dms_datastore & call reformat --inpath raw --outpath formatted'
                }
            }
        }
        stage('usgs multi') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat 'call %CONDA_BIN%\\conda activate dms_datastore & call usgs_multi --fpath formatted'
                }
            }
        }
        stage('build inventory') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat 'call %CONDA_BIN%\\conda activate dms_datastore & call inventory --repo formatted'
                }
            }
        }
        stage('compare formatted') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat 'call %CONDA_BIN%\\conda activate dms_datastore & call compare_directories --base %REPO%/formatted --compare formatted > compare_formatted.txt'
                }
            }
        }
        stage('post'){
            steps {
                script {
                    def now = new Date()
                    BUILD_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                }
                bat "echo All done - ${BUILD_TIME}"
            }
        }
    }
    post {
        success {
            // Actions to perform on success
            mail to: "${env.DATASTORE_ADMIN_EMAILS}", // make sure to define this in the global properties of Jenkins
                    subject: "Passed Pipeline: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                    body: "All good with: ${env.BUILD_URL}"
            echo 'Pipeline completed successfully.'
        }
        failure {
            // Actions to perform on failure
            mail to: "${env.DATASTORE_ADMIN_EMAILS}",
                    subject: "Failed Pipeline: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                    body: "Something is wrong with this build: ${env.BUILD_URL}"
        }
        always {
            // Actions to perform after every run regardless of the result
            echo 'Pipeline finished.'
        }
    }
}
