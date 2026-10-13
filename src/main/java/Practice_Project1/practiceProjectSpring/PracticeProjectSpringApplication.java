package Practice_Project1.practiceProjectSpring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class PracticeProjectSpringApplication {

	public static void main(String[] args) {
		SpringApplication.run(PracticeProjectSpringApplication.class, args);
	}

}




/*git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/username/project-name.git
git push -u origin main*/


//Step 1: Pull remote changes using rebase
//git pull --rebase origin main
//
//
//Step 2: If it completes successfully, push your code
//git push origin main


