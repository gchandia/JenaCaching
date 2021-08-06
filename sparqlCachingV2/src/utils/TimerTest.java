package utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimerTest {
    public static void main(String[] args) {
        ExecutorService service = Executors.newCachedThreadPool();
        Callable<Void> sampleJob = new SampleStudentJob();
        System.out.println("Submitting job");
        Future<Void> token = service.submit(sampleJob);
        try {
            // this call will block for up to 15s
            token.get(2,TimeUnit.SECONDS);
            for (int i = 1; i < 1000000; i++) {
            	System.out.println(i++);
            }
        } catch (InterruptedException e) {
            // this means that your call was interrupted because this current thread (the main thread) was interrupted - shut down gracefully or just ignore
            e.printStackTrace();
        } catch (ExecutionException e) {
            // this means that a problem occurred within the sample job - complain or scold or whatever
            e.printStackTrace();
        } catch (TimeoutException e) {
            // this means that the sample job did not complete within the time period
            e.printStackTrace();
            System.out.println("Interrupting the rude job");
            token.cancel(true);
        }
      }
    private static class SampleStudentJob implements Callable<Void> {
        @Override
        public Void call()
                throws Exception
        {
            System.out.println("Rude job starting");
            try {
                while(true) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                System.out.println("Hey! I was interrupted");
            }
            return null;
        }
    }
}
