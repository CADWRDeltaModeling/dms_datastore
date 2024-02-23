pipeline {
    agent any
    parameters{
        booleanParam(name: 'Full Refresh', defaultValue: true, description: 'Full refresh or partial refresh?')
        booleanParam(name: 'DWR NCRO', defaultValue: true, description: 'Process DWR NCRO?')
        booleanParam(name: 'DWR', defaultValue: true, description: 'Process DWR?')
        booleanParam(name: 'USGS', defaultValue: true, description: 'Process USGS?')
        booleanParam(name: 'NOAA', defaultValue: true, description: 'Process NOAA?')
        booleanParam(name: 'DWR DES', defaultValue: true, description: 'Process DWR DES?')
        booleanParam(name: 'USBR', defaultValue: true, description: 'Process USBR?')
        booleanParam(name: 'CDEC', defaultValue: true, description: 'Process CDEC?')

    }
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
        stage('Ensure Raw Directory') {
            steps {
                dir("${env.REPO_STAGING}"){
                    script {
                        // Check if the 'raw' directory exists
                        if(fileExists('raw')) {
                            // Change directory to 'raw'
                            dir('raw') {
                                // if full refresh, delete all files and directories in 'raw'
                                // if any agency is not being processed, do not delete 'raw'
                                if (params['Full Refresh'] && params['DWR NCRO'] && params['DWR'] && params['USGS'] && params['NOAA'] && params['DWR DES'] && params['USBR']) {
                                    // Delete all files and directories in 'raw'
                                    deleteDir()
                                    echo 'Directory "raw" has been deleted.'
                                } else {
                                    echo 'Partial refresh - not deleting raw directory.'
                                }
                            }
                        } else {
                            echo 'Directory "raw" does not exist!'
                            if (!params['Full Refresh']) { // assumes raw directory exists... else fail!
                                error('Partial refresh requested but "raw" directory does not exist!')
                            } else {
                                echo 'Full refresh requested - creating "raw" directory.'
                                dir('raw') {
                                    // write file with date created
                                    script {
                                        def now = new Date()
                                        CREATE_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                                    }
                                    writeFile file:'created.txt', text:CREATE_TIME
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('Parallel Tasks for Agencies and Variables') {
            parallel {
                stage('DWR NCRO') {
                    when {
                        expression { params['DWR NCRO'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=dwr_ncro --dest=raw-dwr_ncro"
                            }
                        }
                    }
                }
                stage('DWR') {
                    when {
                        expression { params['DWR'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=dwr --dest=raw-dwr"
                            }
                        }
                    }
                }
                stage('USGS') {
                    when {
                        expression { params['USGS'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=usgs --dest=raw-usgs"
                            }
                        }
                    }
                }
                stage('NOAA') {
                    when {
                        expression { params['NOAA'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=noaa --dest=raw-noaa"
                            }
                        }
                    }
                }
                stage('DWR DES') {
                    when {
                        expression { params['DWR DES'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=dwr_des --dest=raw-dwr_des"
                            }
                        }
                    }
                }
                stage('USBR') {
                    when {
                        expression { params['USBR'] }
                    }
                    agent any
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            dir("${env.REPO_STAGING}/rawx"){
                                bat "call %CONDA_BIN%\\conda activate dms_datastore & call populate_repo --agencies=usbr --dest=raw-usbr"
                            }
                        }
                    }
                }
            }
        }
        stage('Consolidate Raw') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    dir("${env.REPO_STAGING}"){
                        bat '''REM Create the raw directory if it doesn't exist
    if not exist raw mkdir raw

    REM Check each raw-* directory before moving and deleting
    for /d %%d in (rawx\\raw-*) do (
        if exist "%%d\\*" (
            REM Move contents from raw-* to raw
            move "%%d\\*" raw\\ || echo Failed to move files from %%d to raw. Maybe empty directory?
        ) else (
            echo %%d is empty. No files to move.
        )
        if exist "%%d" (
            REM Delete raw-* directory
            rmdir /s /q "%%d" || echo Failed to delete %%d. Maybe non-empty directory?
        ) else (
            echo %%d does not exist. Nothing to delete.
        )
    )'''
                    }
                }
            }
        }
        stage('compare raw') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call %CONDA_BIN%\\conda activate dms_datastore & call compare_directories --base %REPO_STAGING_REF%/raw --compare raw > compare_raw.txt'''
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
