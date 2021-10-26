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
		  timeout(time: 10, unit: 'MINUTES')  
	    }
		
           steps {
                sh '''
                mkdir -p build && cd build
                cmake .. -DCMAKE_BUILD_TYPE=Release && make -j
                ./dbtest'''
           }
	   post {
 			   failure {
				     sh '''
				     echo "Running ${env.BUILD_NUMBER} on ${JENKINS_URL}"
		 		     '''
			   }
			   success {
				     sh '''
				     echo "Running ${env.BUILD_NUMBER} on ${JENKINS_URL}"
		 		     '''
			   }
				
	 } 			
        }
       stage('benchmarks') {
	   steps {	
		sh '''
                cd scripts
                python3 basic_benchmarks.py'''
	   }	
       }
	stage('compare') {
	   steps {	
		 sh '''
		 pwd
		 '''
		   
	   }
	   post {
 			   failure {
				     sh '''
				     echo "Running ${env.BUILD_NUMBER} on ${JENKINS_URL}"
		 		     '''
			   }
			   success {
				     sh '''
				     echo "Running ${env.BUILD_NUMBER} on ${JENKINS_URL}"
		 		     '''
			   }
				
	 } 				
	   
       }	    
    }		    
	    
}  
 
