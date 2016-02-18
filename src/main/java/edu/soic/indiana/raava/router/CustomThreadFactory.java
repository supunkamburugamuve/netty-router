package edu.soic.indiana.raava.router;

import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to rename the server threads
 */
public class CustomThreadFactory implements ThreadFactory {
    static {
        // Rename Netty threads
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
    }

    private final ThreadGroup group;
    private final AtomicInteger index = new AtomicInteger(1);
    private final String name;

    public CustomThreadFactory(String name) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.name = name;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, name + "-" + index.getAndIncrement(), 0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
