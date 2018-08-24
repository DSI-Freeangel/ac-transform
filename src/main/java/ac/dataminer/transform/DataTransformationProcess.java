package ac.dataminer.transform;

import ac.dataminer.transform.exception.RowFilteredOutException;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Describes process that can be executed sync or async mode.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class DataTransformationProcess {
    private final DataGenerator dataGenerator;
    private final RowTransformation transformation;
    private final ResultsConsumer consumer;
    private Flowable<Map<String, String>> process;
    private Disposable disposable;
    private CountDownLatch completionCounter;

    /**
     * Starts asynchronous processing
     */
    public void start() {
        if(isActive()) {
            log.info("Process is already started");
            return;
        }
        completionCounter = new CountDownLatch(1);
        disposable = getProcess()
                .subscribeOn(Schedulers.io())
                .subscribe(consumer, this::handleProcessError, this::complete);
        log.info("Process started");
    }

    /**
     * Call synchronous execution of process. Waits until process will be completed.
     */
    public void run() {
        start();
        awaitCompletion();
    }

    /**
     * Used to stop asynchronous process execution.
     */
    public void stop() {
        if(isActive()) {
            disposable.dispose();
            disposable = null;
            log.info("Process stopped");
        } else {
            log.info("Process is not active");
        }
    }

    /**
     * Used to wait until asynchronous process will be completed.
     */
    public void awaitCompletion() {
        if(null != completionCounter) {
            try {
                completionCounter.await();
            } catch (InterruptedException e) {
                log.info("Process interrupted");
            }
        }
    }

    /**
     * Check state of process
     * @return true in case process is not completed or stopped yet.
     */
    public boolean isActive() {
        return Optional.ofNullable(disposable).map(d -> !d.isDisposed()).orElse(false);
    }

    public static DataTransformationProcessBuilder builder() {
        return new DataTransformationProcessBuilder();
    }

    private Flowable<Map<String, String>> getProcess() {
        return null != process ? process : buildProcess();
    }

    private Flowable<Map<String, String>> buildProcess() {
        process = Flowable.generate(dataGenerator)
                .parallel()
                .runOn(Schedulers.computation())
                .flatMap(Flowable::fromIterable)
                .flatMap(this::transformRow)
                .sequential();
        return process;
    }

    private Flowable<Map<String, String>> transformRow(Map<String, String> row) {
        return Flowable.fromArray(row)
                .map(transformation::apply)
                .onErrorResumeNext(this::handleRowTransformationErrors);
    }

    private Flowable<Map<String, String>> handleRowTransformationErrors(Throwable throwable) {
        if(!RowFilteredOutException.class.isAssignableFrom(throwable.getClass())) {
            log.error("Row transformation error", throwable);
        }
        return Flowable.empty();
    }

    private void handleProcessError(Throwable throwable) {
        log.error("Process terminated with error", throwable);
        completionCounter.countDown();
    }

    private void complete() throws IOException {
        log.info("Process complete");
        consumer.complete();
        completionCounter.countDown();
    }
}
