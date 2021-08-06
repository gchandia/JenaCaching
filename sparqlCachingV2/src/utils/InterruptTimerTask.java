package utils;

import java.util.TimerTask;

/*
 * A TimerTask that interrupts the specified thread when run.
 */
public class InterruptTimerTask extends TimerTask {

	private Thread theTread;
    private long timeout;

    public InterruptTimerTask(Thread theTread,long i_timeout) {
        this.theTread = theTread;
        timeout=i_timeout;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
        theTread.interrupt();
    }
}