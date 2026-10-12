
package Practice_Project1.practiceProjectSpring.Consumer;

import Practice_Project1.practiceProjectSpring.DTO.EmployeeEvent;
import Practice_Project1.practiceProjectSpring.Service.EmployeeProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Service
@RequiredArgsConstructor

public class EmployeeKafkaConsumer {

    private final EmployeeProcessingService employeeProcessingService ;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    @KafkaListener(
            topics = "employee-events",
            groupId = "employee-group"
    )
    public void consume(EmployeeEvent event) {

        executorService.submit(
                ()-> employeeProcessingService.process(event)
        );

//        System.out.println(
//                "Received Employee Event: " + event
//        );
    }
}