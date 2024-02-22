pipeline {
    agent any
    environment {
        //Location of the repository
        REPO='y:\\repo\\continuous'
        REPO_STAGING='y:\\jenkins_repo_staging\\continuous'
        // Declaring screening variables
        SCREEN_CONFIG = 'screen_config_v20230126'
        SRCDIR = 'formatted'
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
        stage('ensure directories') {
            steps {
                dir("${env.REPO_STAGING}/plots"){
                    // write file with date created
                    script {
                        def now = new Date()
                        CREATE_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                    }
                    writeFile file:'created.txt', text:CREATE_TIME
                }
                dir("${env.REPO_STAGING}/screened"){
                    // write file with date created
                    script {
                        def now = new Date()
                        CREATE_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                    }
                    writeFile file:'created.txt', text:CREATE_TIME
                }
            }
        }
        stage('auto screen') {
            parallel {
                stage('Elev') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params elev"
                        }
                    }
                }
                stage('Flow') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params flow"
                        }
                    }
                }
                stage('Ec') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ec"
                        }
                    }
                }
                stage('Temp') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params temp"
                        }
                    }
                }
                stage('Turbidity') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params turbidity"
                        }
                    }
                }
                stage('Ssc') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ssc"
                        }
                    }
                }
                stage('Ph') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ph"
                        }
                    }
                }
                stage('Do') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params do"
                        }
                    }
                }
                stage('Predictions') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params predictions"
                        }
                    }
                }
                stage('Cla') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params cla"
                        }
                    }
                }
                stage('Velocity') {
                    agent any
                    steps {
                        dir("${env.REPO_STAGING}"){
                            bat "call %CONDA_BIN%\\conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params velocity"
                        }
                    }
                }
            }
        }
        stage('compare screened') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call %CONDA_BIN%\\conda activate dms_datastore & call compare_directories --base %REPO%/screened --compare screened > compare_screened.txt'''
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
