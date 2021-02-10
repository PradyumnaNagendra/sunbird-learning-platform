package org.sunbird.jobs.samza.task;
/**
 * @author Pradyumna
 */

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.ekstep.jobs.samza.service.ISamzaService;
import org.ekstep.jobs.samza.util.JobLogger;
import org.sunbird.jobs.samza.service.CertificateGeneratorService;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CourseCertificateGeneratorTask extends BaseTask {

    private static JobLogger LOGGER = new JobLogger(CourseCertificateGeneratorTask.class);
    private ISamzaService service = new CertificateGeneratorService();
    private AtomicInteger courseBatchCounter = new AtomicInteger(0);
    private AtomicInteger enrolmentCounter = new AtomicInteger(0);
    private AtomicInteger assessmentCounter = new AtomicInteger(0);
    
    @Override
    public ISamzaService initialize() throws Exception {
        LOGGER.info("Task initialized");
        this.action = Arrays.asList("generate-course-certificate", "issue-certificate");
        this.jobStartMessage = "Started processing of course-certificate-generator samza job";
        this.jobEndMessage = "course-certificate-generator job processing complete";
        this.jobClass = "org.sunbird.jobs.samza.task.CourseCertificateGeneratorTask";
        return service;
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        try {
            LOGGER.info("Starting to process for mid : " + message.get("mid") + " at :: " + System.currentTimeMillis());
            service.processMessage(message, metrics, collector);
            LOGGER.info("Successfully completed processing  for mid : " + message.get("mid") + " at :: " + System.currentTimeMillis());
        } catch (Exception e) {
            metrics.incErrorCounter();
            LOGGER.error("Message processing failed", message, e);
        }
    }
    
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        super.window(collector, coordinator);
        System.out.println("CourseBatch counter: " + courseBatchCounter.get() + "\t Enrolments counter : " + enrolmentCounter.get()  + "\t assessment counter : " + assessmentCounter.get());
    }
}
