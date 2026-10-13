



package Practice_Project1.practiceProjectSpring.Service;

import Practice_Project1.practiceProjectSpring.DTO.EmployeeEvent;
import org.springframework.stereotype.Service;

@Service
public class EmployeeProcessingService {

    public void process(EmployeeEvent event) {



//        if (event.getEmployeeId() == 999) {
//            throw new RuntimeException("Employee processing failed");
//        }

        System.out.println(
                Thread.currentThread().getName()
                        + " processing "
                        + event
        );

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println(
                Thread.currentThread().getName()
                        + " completed "
                        + event
        );
    }
}