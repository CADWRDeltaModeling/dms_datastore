pipeline {
    agent any
    environment {
        //Location of the repository
        REPO='y:\\repo\\continuous'
        REPO_STAGING='y:\\jenkins_repo_staging\\continuous'
        // Declaring screening variables
        SCREEN_CONFIG = 'screen_config_v20230126'
        SRCDIR = 'formatted'
    }
    stages {
        stage('mount network drive') {
            steps {
                script {
                    // Define the network path and drive letter
                    def networkPath = '\\\\cnrastore-bdo\\modeling_data'
                    def driveLetter = 'Y:'

                    // Check if the drive is already mounted
                    def isMounted = bat(script: "if exist ${driveLetter} (echo true) else (echo false)", returnStdout: true).trim()

                    // Mount the network drive if it's not already mounted
                    if (isMounted == 'false') {
                        bat "net use ${driveLetter} ${networkPath} /persistent:no"
                        echo "Mounted ${networkPath} as ${driveLetter}"
                    } else {
                        echo "${driveLetter} is already mounted."
                    }
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
            }
        }
        stage('auto screen') {
            steps {
                dir("${env.REPO_STAGING}"){
                    script {
                        // Running commands in parallel
                        parallel(
                            elev: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params elev"
                            },
                            flow: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params flow"
                            },
                            ec: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ec"
                            },
                            temp: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params temp"
                            },
                            turbidity: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params turbidity"
                            },
                            ssc: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ssc"
                            },
                            ph: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params ph"
                            },
                            do: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params do"
                            },
                            predictions: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params predictions"
                            },
                            cla: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params cla"
                            },
                            velocity: {
                                bat "call conda activate dms_datastore & auto_screen --config %SCREEN_CONFIG% --fpath %SRCDIR% --dest screened --plot_dest plots --params velocity"
                            }
                        )
                    }
                }
            }
        }
        stage('compare screened') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call conda activate dms_datastore & call compare_directories --base %REPO%/screened --compare screened'''
                }
            }
        }
        stage('post'){
            steps {
                script {
                    def now = new Date()
                    BUILD_TIME=now.format("yyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
                }
                bat "All done - ${BUILD_TIME}"
            }
        }
    }
}
