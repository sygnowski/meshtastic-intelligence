pipeline {
    agent any

    parameters {
        string(
            name: 'EXTRA_OPTS',
            defaultValue: '--no-build-cache --no-daemon --console=plain --info',
            description: 'Gradle Extra Options'
        )
    }

    stages {
        stage('Build') {
            steps {
                sh "chmod u+x ./gradlew"
                sh "./gradlew ${params.EXTRA_OPTS} -x shadowJar -x distZip -x distTar build"
            }
        }
    }
}
