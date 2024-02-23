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
}
