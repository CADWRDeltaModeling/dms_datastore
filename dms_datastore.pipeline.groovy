pipeline {
    agent any
    environment {
        //Location of the repository
        REPO='y:\\repo\\continuous'
        REPO_STAGING='y:\\jenkins_repo_staging\\continuous'
    }
    stages {
        stage('Mount Network Drive') {
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

                    bat 'dir Y:\\'
                }
            }
        }
        stage('Check and Delete Directory') {
            steps {
                dir("${env.REPO_STAGING}"){
                    script {
                        // Check if the 'raw' directory exists
                        if(fileExists('raw')) {
                            // Change directory to 'raw'
                            dir('raw') {
                                // Delete the 'raw' directory
                                deleteDir()
                            }
                            echo 'Directory "raw" has been deleted.'
                        } else {
                            echo 'Directory "raw" does not exist.'
                        }
                    }
                }
            }
        }

        stage('Parallel Tasks for Agencies and Variables') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    dir("${env.REPO_STAGING}"){
                        script {
                            def allAgencies = ["usgs", "dwr_des", "usbr", "noaa", "dwr_ncro", "dwr"]

                            // Prepare a map for parallel execution
                            def parallelTasks = [:]

                            // Loop over each agency
                            allAgencies.each { agency ->
                                def varlist = []
                                if (agency == "noaa") {
                                    varlist = ["elev", "predictions"]
                                } else {
                                    varlist = ["flow", "elev", "ec", "temp", "do", "turbidity", "velocity", "ph", "ssc"]
                                }

                                // Loop over each variable for the agency
                                varlist.each { variable ->
                                    def taskName = "${agency}_${variable}"
                                    parallelTasks[taskName] = {
                                        // Activate the conda environment and run the populate_repo script
                                        echo "Processing ${agency} with variable ${variable}"
                                        bat "call conda activate dms_datastore & mkdir raw-${agency}-${variable} & call populate_repo --agencies=${agency} --variables=${variable} --dest=raw-${agency}-${variable}"
                                    }
                                }
                            }

                            // Run tasks in parallel
                            parallel parallelTasks
                        }
                    }
                }
            }
        }
        stage('Consolidate Raw') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''
                        REM Create the raw directory if it doesn't exist
                        if not exist raw mkdir raw

                        REM Move contents from raw-* to raw and delete raw-* directories
                        for /d %%d in (raw-*) do (
                            move "%%d\\*" raw\\
                            rmdir /s /q "%%d"
                        )
                    '''
                }
            }
        }
        /**
        stage('compare raw') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call conda activate dms_datastore & call compare_directories --base %REPO%/raw --compare raw > compare_raw.txt'''
                }
            }
        }
        **/
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
            steps {
                script {
                    parallel(
                        usgs: {
                            dir("${env.REPO_STAGING}") {
                                bat 'call conda activate dms_datastore & call reformat --inpath raw --outpath formatted --agencies=usgs'
                                bat '''call conda activate dms_datastore & call usgs_multi --fpath formatted'''
                            }
                        },
                        des: {
                            dir("${env.REPO_STAGING}") {
                                bat 'call conda activate dms_datastore & call reformat --inpath raw --outpath formatted --agencies=des'
                            }
                        },
                        cdec: {
                            dir("${env.REPO_STAGING}") {
                                bat 'call conda activate dms_datastore & call reformat --inpath raw --outpath formatted --agencies=cdec'
                            }
                        },
                        noaa: {
                            dir("${env.REPO_STAGING}") {
                                bat 'call conda activate dms_datastore & call reformat --inpath raw --outpath formatted --agencies=noaa'
                            }
                        },
                        ncro: {
                            dir("${env.REPO_STAGING}") {
                                bat 'call conda activate dms_datastore & call reformat --inpath raw --outpath formatted --agencies=ncro'
                            }
                        }
                    )
                }
            }
        }
        stage('build inventory') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call conda activate dms_datastore & call inventory --repo formatted'''
                }
            }
        }
        stage('compare formatted') {
            steps {
                dir("${env.REPO_STAGING}"){
                    bat '''call conda activate dms_datastore & call compare_directories --base %REPO%/formatted --compare formatted'''
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
