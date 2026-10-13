
package Practice_Project1.practiceProjectSpring.Consumer;

import Practice_Project1.practiceProjectSpring.DTO.EmployeeEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmployeeDltConsumer {

    @KafkaListener(
            topics = "employee-events.DLT",
            groupId = "employee-dlt-group"
    )
    public void consumeFailedEvent(EmployeeEvent event) {

        System.out.println("Failed event received in DLT: " + event);
    }
}