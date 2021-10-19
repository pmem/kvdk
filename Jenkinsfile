pipeline {
    agent {
      label 'AEP-wf-01'
    }
    options {
        timestamps() //日志时间
	disableConcurrentBuilds()   //不允许两个job同时执行
	buildDiscarder(logRotator(numToKeepStr: '30'))   //日志保留30个 
		
    }		

    stages {
        stage('dbtest') {
	    options { 
		  retry(3)
		  timeout(time: 1, unit: 'HOURS')
		  
	    }
		
           steps {
                sh '''
                mkdir -p build && cd build
                cmake .. -DCMAKE_BUILD_TYPE=Release && make -j
                ./dbtest'''
           }
        }   
    }
	post {

	  failure {
				                
               sh '''
                 pwd
                 '''
      }	  

    } 
	stages {
        stage('benchmarks') { sh '''
                cd scripts
                python3 basic_benchmarks.py'''
   
    }


}
