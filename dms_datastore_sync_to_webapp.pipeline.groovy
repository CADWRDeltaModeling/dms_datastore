pipeline {
    agent any

    environment {
        // An azure service principal is used to authenticate to Azure
        // credentials for those are set in Jenkins
        // storeage account key is set as a secret in Jenkins
        AZ_PATH='d:\\Programs\\azure-cli-2.57.0-x64\\bin'
        AZCOPY_PATH='D:\\Programs\\azcopy_windows_amd64_10.23.0'
        AZURE_CREDENTIAL_ID = 'azure-dms-datastore-service-principal'
        // Below are the parameters that need to be set in the Jenkins job
        //SUBSCRIPTION_ID parameter
        //RESOURCE_GROUP parameter
        //WEB_APP_NAME parameter
        //STORAGE_ACCOUNT_NAME parameter
        SOURCE_DIRECTORY = 'Y:\\jenkins_repo_staging\\continuous' 
        DESTINATION_DIRECTORY = 'dms_datastore_ui/continuous'
        FILE_SHARE_NAME = 'data'
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
        stage('Copy Files to File Share') {
            steps {
                script {
                    // Nest withCredentials if using different credential types
                    withCredentials([azureServicePrincipal(credentialsId: 'azure-dms-datastore-service-principal')]) {
                        // Inner withCredentials for string secrets
                        withCredentials([string(credentialsId: 'dwrbdodashstore-key1', variable: 'AZURE_STORAGE_KEY')]) {
                            powershell '''
                            # Login to Azure
                            $azPath = "$env:AZ_PATH"

                            & "$azPath\\az" login --service-principal -u $env:AZURE_CLIENT_ID -p $env:AZURE_CLIENT_SECRET --tenant $env:AZURE_TENANT_ID
                            Write-Host "Logged in to Azure"

                            # Set storage account key
                            $env:STORAGE_ACCOUNT_KEY = "$env:AZURE_STORAGE_KEY"

                            # Generate SAS token
                            $fileShareName = "$env:FILE_SHARE_NAME"
                            $storageAccountName = "$env:STORAGE_ACCOUNT_NAME"
                            # Generate SAS token
                            # Calculate the expiry date 1 day from now
                            $sasExpiryTime = (Get-Date).AddDays(1).ToString("yyyy-MM-ddTHH:mm:ssZ")

                            $sasToken = & "$azPath\\az" storage share generate-sas --name $fileShareName --account-name $storageAccountName --account-key $env:STORAGE_ACCOUNT_KEY --permissions dlrw --expiry $sasExpiryTime -o tsv
                            Write-Host "SAS Token generated: $sasToken"

                            # Set azcopy log location to current directory
                            $env:AZCOPY_LOG_LOCATION = Get-Location

                            # Upload all files in the screened directory
                            $sourceDirectory = "$env:SOURCE_DIRECTORY"
                            $destinationDirectory = "$env:DESTINATION_DIRECTORY"
                            $azCopyPath = "$env:AZCOPY_PATH"

                            Write-Host "Uploading all files in the screened directory"
                            & "$azCopyPath\\azcopy" copy "$sourceDirectory\\screened\\*" "https://$storageAccountName.file.core.windows.net/$fileShareName/$destinationDirectory/screened/?$sasToken" --recursive=true
                            Write-Host "Data files copied to File Share"

                            # Upload all CSV files in the source directory
                            Write-Host "Uploading all CSV files in the source directory"
                            & "$azCopyPath\\azcopy" copy "$sourceDirectory\\*.csv" "https://$storageAccountName.file.core.windows.net/$fileShareName/$destinationDirectory/?$sasToken" --recursive=true
                            Write-Host "Inventory CSV files copied to File Share"
                            '''
                        }
                    }
                }
            }
        }
        stage('Restart Web App') {
            steps {
                script {
                    withCredentials([azureServicePrincipal(credentialsId: AZURE_CREDENTIAL_ID)]) {
                        withCredentials([string(credentialsId: 'dwrbdodashstore-key1', variable: 'AZURE_STORAGE_KEY')]) {
                            bat '''
                            call %AZ_PATH%\\az login --service-principal -u %AZURE_CLIENT_ID% -p %AZURE_CLIENT_SECRET% --tenant %AZURE_TENANT_ID%
                            echo Logged in to Azure
                            set AZURE_STORAGE_ACCOUNT=%STORAGE_ACCOUNT_NAME%
                            set AZURE_STORAGE_KEY=%AZURE_STORAGE_KEY%
                            call %AZ_PATH%\\az storage file delete --path dms_datastore_ui/caching.log --share-name %FILE_SHARE_NAME% --account-name %STORAGE_ACCOUNT_NAME%
                            echo Deleted caching.log from File Share
                            call %AZ_PATH%\\az webapp restart --name %WEB_APP_NAME% --resource-group %RESOURCE_GROUP%
                            echo Web App restarted
                            '''
                        }
                    }
                    echo 'Waiting for 30 seconds...'
                    sleep 30
                }
            }
        }
        stage('Ping Website') {
            steps {
                script {
                    powershell '''
                    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
                    $webappname = "$env:WEB_APP_NAME"
                    $url = "https://$webappname.azurewebsites.net/repoui" #
                    try {
                        $response = Invoke-WebRequest -Uri $url -UseBasicParsing
                        if ($response.StatusCode -eq 200) {
                            Write-Output "Website is reachable. Status Code: 200 OK"
                        } else {
                            Write-Output "Website responded with Status Code: $($response.StatusCode)"
                        }
                    } catch {
                        Write-Error "Website is not reachable. Error: $($_.Exception.Message)"
                        exit 1
                    }
                    '''
                }
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
