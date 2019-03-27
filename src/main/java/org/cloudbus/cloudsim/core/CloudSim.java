/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.core;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.events.*;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.network.topologies.NetworkTopology;
import org.cloudbus.cloudsim.util.Log;

import java.util.function.Predicate;

import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.EventListener;

import static java.util.stream.Collectors.toList;

/**
 * The main class of the simulation API, that manages Cloud Computing simulations providing all methods to
 * start, pause and stop them. It sends and processes all discrete events during the simulation time.
 *
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Toolkit 1.0
 */
public class CloudSim implements Simulation {

    /**
     * CloudSim Plus current version.
     */
    public static final String VERSION = "2.2.2";

    /**
     * An array that works as a circular queue with capacity for just 2 elements
     * (defined in the constructor). When a new element is added to the queue,
     * the first element is removed to open space for that new one.
     * This queue stores the last 2 simulation clock values.
     * It is used to know when it's time to notify listeners that
     * the simulation clock has incremented.
     *
     * <p>The head (value at index 0) of the queue is the oldest simulation time stored,
     * the tail (value at index 1) is the newest one.</p>
     *
     * <p>Such a structure is required because multiple events
     * can be received consecutively for the same simulation time.
     * When the head of the queue is lower than the tail,
     * it means the last event for that head time
     * was already processed and a more recent event
     * has just arrived.
     * </p>
     *
     * @see #notifyOnClockTickListenersIfClockChanged()
     */
    private final double[] circularClockTimeQueue;

    /**
     * The last time the OnClockTickListeners were updated.
     *
     * @see #addOnClockTickListener(EventListener)
     */
    private double lastTimeClockTickListenersUpdated;

    /**
     * @see #getNetworkTopology()
     */
    private NetworkTopology networkTopology;

    /**
     * The Cloud Information Service (CIS) entity.
     */
    private final CloudInformationService cis;

    /**
     * The calendar.
     */
    private final Calendar calendar;

    /**
     * @see #getMinTimeBetweenEvents()
     */
    private final double minTimeBetweenEvents;

    /**
     * @see #getEntityList()
     */
    private final List<CloudSimEntity> entities;

    /**
     * The queue of events that will be sent in a future simulation time.
     */
    private final FutureQueue future;

    /**
     * The deferred event queue.
     */
    private final DeferredQueue deferred;

    /**
     * The current simulation clock.
     */
    private double clockTime;

    /**
     * @see #isRunning()
     */
    private boolean running;

    /**
     * A map of entities and predicates that define the events a given entity is waiting for.
     * Received events are filtered based on the predicate associated with each entity
     * so that just the resulting events are sent to the entity.
     */
    private final Map<SimEntity, Predicate<SimEvent>> waitPredicates;

    /**
     * @see #isPaused()
     */
    private boolean paused;

    /**
     * Indicates the time that the simulation has to be paused.
     * -1 means no pause was requested.
     */
    private double pauseAt = -1;

    private final Set<EventListener<SimEvent>> onEventProcessingListeners;
    private final Set<EventListener<EventInfo>> onSimulationPausedListeners;
    private final Set<EventListener<EventInfo>> onClockTickListeners;

    /**
     * Creates a CloudSim simulation.
     * Internally it creates a CloudInformationService.
     *
     * @see CloudInformationService
     * @see #CloudSim(double)
     */
    public CloudSim() {
        this(0.1);
    }

    /**
     * Creates a CloudSim simulation that tracks events happening in a time interval
     * as little as the minTimeBetweenEvents parameter.
     * Internally it creates a {@link CloudInformationService}.
     *
     * @param minTimeBetweenEvents the minimal period between events. Events
     *                             within shorter periods after the last event are discarded.
     * @see CloudInformationService
     */
    public CloudSim(final double minTimeBetweenEvents) {
        this.entities = new ArrayList<>();
        this.future = new FutureQueue();
        this.deferred = new DeferredQueue();
        this.waitPredicates = new HashMap<>();
        this.networkTopology = NetworkTopology.NULL;
        this.clockTime = 0;
        this.running = false;
        this.onEventProcessingListeners = new HashSet<>();
        this.onSimulationPausedListeners = new HashSet<>();
        this.onClockTickListeners = new HashSet<>();

        // NOTE: the order for the lines below is important
        this.calendar = Calendar.getInstance();
        this.cis = new CloudInformationService(this);

        if (minTimeBetweenEvents <= 0) {
            throw new IllegalArgumentException("The minimal time between events should be positive, but is:" + minTimeBetweenEvents);
        }

        this.minTimeBetweenEvents = minTimeBetweenEvents;

        this.lastTimeClockTickListenersUpdated = minTimeBetweenEvents;
        this.circularClockTimeQueue = new double[]{minTimeBetweenEvents, minTimeBetweenEvents};
    }

    @Override
    public double start() {
        Log.printConcatLine("Starting CloudSim Plus ", VERSION);
        runStart();
        return 0.0;
    }

    @Override
    public boolean terminate() {
        if (running) {
            running = false;
            return true;
        }

        return false;
    }

    @Override
    public boolean terminateAt(final double time) {
        if (time <= clockTime) {
            return false;
        }

        return true;
    }

    @Override
    public double getMinTimeBetweenEvents() {
        return minTimeBetweenEvents;
    }

    @Override
    public Calendar getCalendar() {
        return calendar;
    }

    @Override
    public CloudInformationService getCloudInfoService() {
        return cis;
    }

    @Override
    public Set<Datacenter> getDatacenterList() {
        return cis.getDatacenterList();
    }

    @Override
    public double clock() {
        return clockTime;
    }

    @Override
    public double clockInMinutes() {
        return clock() / 60.0;
    }

    @Override
    public double clockInHours() {
        return clock() / 3600.0;
    }

    /**
     * Updates the simulation clock
     *
     * @param newTime simulation time to set
     * @return the old simulation time
     */
    private double setClock(final double newTime) {
        final double oldTime = clockTime;
        this.clockTime = newTime;
        return oldTime;
    }

    @Override
    public int getNumEntities() {
        return entities.size();
    }

    @Override
    public List<SimEntity> getEntityList() {
        return Collections.unmodifiableList(entities);
    }

    @Override
    public void addEntity(final CloudSimEntity e) {
        if (running) {
            //@todo src 1, dest 0? What did it mean? Probably nothing.
            final SimEvent evt = new CloudSimEvent(this, SimEvent.Type.CREATE, clockTime, e);
            future.addEvent(evt);
        }

        if (e.getId() == -1) { // Only add once!
            e.setId(entities.size());
            entities.add(e);
        }
    }

    private void processAllFutureEventsHappeningAtSameTimeOfTheFirstOne(final SimEvent firstEvent) {
        long start1 = System.currentTimeMillis();
        processEvent(firstEvent);
        future.remove(firstEvent);
        long stop1 = System.currentTimeMillis();

        long start2 = System.currentTimeMillis();
        final List<SimEvent> eventsToProcess = future.stream()
                .filter(e -> e.eventTime() == firstEvent.eventTime())
                .collect(toList());
        long stop2 = System.currentTimeMillis();

        long start3 = System.currentTimeMillis();
        for (final SimEvent evt : eventsToProcess) {
            processEvent(evt);
            future.remove(evt);
        }
        long stop3 = System.currentTimeMillis();

//        System.out.println(String.format(
//                ">>>>>>>>>>> STATS processAllFutureEventsHappeningAtSameTimeOfTheFirstOne: 1 %s s 2 %s s 3 %s s",
//                (stop1 - start1) / 1000.0,
//                (stop2 - start2) / 1000.0,
//                (stop3 - start3) / 1000.0));
    }

    /**
     * Gets the list of entities that are in {@link SimEntity.State#RUNNABLE}
     * and execute them.
     */
    private void executeRunnableEntities() {
        final List<SimEntity> runnableEntities = entities.stream()
                .filter(ent -> ent.getState() == SimEntity.State.RUNNABLE)
                .collect(toList());

        runnableEntities.forEach(SimEntity::run);
    }

    @Override
    public void sendNow(final SimEntity src, final SimEntity dest, final int tag, final Object data) {
        send(src, dest, 0, tag, data);
    }

    @Override
    public void send(final SimEntity src, final SimEntity dest, final double delay, final int tag, final Object data) {
        validateDelay(delay);
        final SimEvent evt = new CloudSimEvent(this, SimEvent.Type.SEND, clockTime + delay, src, dest, tag, data);
        future.addEvent(evt);
    }

    @Override
    public void sendFirst(final SimEntity src, final SimEntity dest, final double delay, final int tag, final Object data) {
        validateDelay(delay);
        final SimEvent evt = new CloudSimEvent(this, SimEvent.Type.SEND, clockTime + delay, src, dest, tag, data);
        future.addEventFirst(evt);
    }

    private void validateDelay(final double delay) {
        if (delay < 0) {
            throw new IllegalArgumentException("Send delay can't be negative.");
        }
    }

    @Override
    public void wait(final CloudSimEntity src, final Predicate<SimEvent> p) {
        src.setState(SimEntity.State.WAITING);
        if (p != ANY_EVT) {
            // If a predicate has been used, store it in order to check incoming events that matches it
            waitPredicates.put(src, p);
        }
    }

    @Override
    public long waiting(final SimEntity dest, final Predicate<SimEvent> p) {
        return filterEventsToDestinationEntity(deferred, p, dest).count();
    }

    @Override
    public SimEvent select(final SimEntity dest, final Predicate<SimEvent> p) {
        final SimEvent evt = findFirstDeferred(dest, p);
        deferred.remove(evt);
        return evt;
    }

    @Override
    public SimEvent findFirstDeferred(final SimEntity dest, final Predicate<SimEvent> p) {
        return filterEventsToDestinationEntity(deferred, p, dest).findFirst().orElse(SimEvent.NULL);
    }

    /**
     * Gets a stream of events inside a specific queue that match a given predicate
     * and are targeted to an specific entity.
     *
     * @param queue the queue to get the events from
     * @param p     the event selection predicate
     * @param dest  Id of entity that the event has to be sent to
     * @return a Stream of events from the queue
     */
    private Stream<SimEvent> filterEventsToDestinationEntity(final EventQueue queue, final Predicate<SimEvent> p, final SimEntity dest) {
        return filterEvents(queue, p.and(e -> e.getDestination() == dest));
    }

    @Override
    public SimEvent cancel(final SimEntity src, final Predicate<SimEvent> p) {
        final SimEvent evt = future.stream().filter(p.and(e -> e.getSource().equals(src))).findFirst().orElse(SimEvent.NULL);
        future.remove(evt);
        return evt;
    }

    @Override
    public boolean cancelAll(final SimEntity src, final Predicate<SimEvent> p) {
        final int previousSize = future.size();
        final List<SimEvent> cancelList = filterEventsFromSourceEntity(src, p, future).collect(toList());
        future.removeAll(cancelList);
        return previousSize < future.size();
    }

    /**
     * Gets a stream of events inside a specific queue that match a given predicate
     * and from a source entity.
     *
     * @param src   Id of entity that scheduled the event
     * @param p     the event selection predicate
     * @param queue the queue to get the events from
     * @return a Stream of events from the queue
     */
    private Stream<SimEvent> filterEventsFromSourceEntity(final SimEntity src, final Predicate<SimEvent> p, final EventQueue queue) {
        return filterEvents(queue, p.and(e -> e.getSource().equals(src)));
    }

    /**
     * Gets a stream of events inside a specific queue that match a given predicate.
     *
     * @param queue the queue to get the events from
     * @param p     the event selection predicate
     * @return a Strem of events from the queue
     */
    private Stream<SimEvent> filterEvents(final EventQueue queue, final Predicate<SimEvent> p) {
        return queue.stream().filter(p);
    }

    /**
     * Processes an event.
     *
     * @param e the event to be processed
     */
    private void processEvent(final SimEvent e) {
        // Update the system's clock
        if (e.eventTime() < clockTime) {
            throw new IllegalArgumentException("Past event detected.");
        }
        setClock(e.eventTime());

        processEventByType(e);
        notifyOnClockTickListenersIfClockChanged();
        notifyOnEventProcessingListeners(e);
    }

    /**
     * Notifies all Listeners about onClockTick event when the simulation clock changes.
     * If multiple events are received consecutively but for the same simulation time,
     * it will only notify the Listeners when the last event for that time is received.
     * It ensures when Listeners receive the notification, all the events
     * for such a simulation time were already processed and then,
     * the Listeners will have access to the most updated simulation state.
     */
    private void notifyOnClockTickListenersIfClockChanged() {
        if (clockTime > lastTimeClockTickListenersUpdated) {
            addCurrentTimeToCircularQueue();
            if (circularClockTimeQueue[0] < circularClockTimeQueue[1]) {
                lastTimeClockTickListenersUpdated = circularClockTimeQueue[0];
                onClockTickListeners.forEach(l -> l.update(EventInfo.of(l, lastTimeClockTickListenersUpdated)));
            }
        }
    }

    /**
     * Makes the circular queue to rotate, removing the first time,
     * then adding the current clock time.
     */
    private void addCurrentTimeToCircularQueue() {
        circularClockTimeQueue[0] = circularClockTimeQueue[1];
        circularClockTimeQueue[1] = clockTime;
    }

    private void processEventByType(final SimEvent e) {
        switch (e.getType()) {
            case NULL:
                throw new IllegalArgumentException("Event has a null type.");
            case CREATE:
                processCreateEvent(e);
                break;
            case SEND:
                processSendEvent(e);
                break;
            case HOLD_DONE:
                processHoldEvent(e);
                break;
        }
    }

    private void processCreateEvent(final SimEvent e) {
        addEntityDynamically((SimEntity) e.getData());
    }

    /**
     * Internal method used to add a new entity to the simulation when the
     * simulation is running.
     *
     * <b>It should not be called from user simulations.</b>
     *
     * @param e The new entity
     */
    protected void addEntityDynamically(final SimEntity e) {
        Objects.requireNonNull(e);
        printMessage("Adding: " + e.getName());
        e.start();
    }

    private void processHoldEvent(final SimEvent e) {
        if (e.getSource() == SimEntity.NULL) {
            throw new IllegalArgumentException("Null entity holding.");
        }

        e.getSource().setState(SimEntity.State.RUNNABLE);
    }

    private void processSendEvent(final SimEvent e) {
        if (e.getDestination() == SimEntity.NULL) {
            throw new IllegalArgumentException("Attempt to send to a null entity detected.");
        }

        final CloudSimEntity destEnt = entities.get(e.getDestination().getId());
        if (destEnt.getState() == SimEntity.State.WAITING) {
            final Predicate<SimEvent> p = waitPredicates.get(destEnt);
            if (p == null || e.getTag() == 9999 || p.test(e)) {
                destEnt.setEventBuffer(new CloudSimEvent(e));
                destEnt.setState(SimEntity.State.RUNNABLE);
                waitPredicates.remove(destEnt);
            } else {
                deferred.addEvent(e);
            }

            return;
        }

        deferred.addEvent(e);
    }

    /**
     * Notifies all registered listeners when a {@link SimEvent} is processed by the simulation.
     *
     * @param e the processed event
     */
    private void notifyOnEventProcessingListeners(final SimEvent e) {
        onEventProcessingListeners.forEach(l -> l.update(e));
    }

    /**
     * Internal method used to start the simulation. This method should
     * <b>not</b> be used by user simulations.
     */
    private void runStart() {
        running = true;
        entities.forEach(SimEntity::start);
        printMessage("Entities started.");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean pause() {
        return pause(clockTime);
    }

    @Override
    public boolean pause(final double time) {
        if (time < clockTime) {
            return false;
        } else {
            pauseAt = time;
            return true;
        }
    }

    @Override
    public boolean resume() {
        final boolean wasPaused = this.paused;
        this.paused = false;

        if (pauseAt <= clockTime) {
            pauseAt = -1;
        }

        return wasPaused;
    }

    @Override
    public void pauseEntity(final SimEntity src, final double delay) {
        final SimEvent evt = new CloudSimEvent(this, SimEvent.Type.HOLD_DONE, clockTime + delay, src);
        future.addEvent(evt);
        src.setState(SimEntity.State.HOLDING);
    }

    @Override
    public void holdEntity(final SimEntity src, final long delay) {
        final SimEvent evt = new CloudSimEvent(this, SimEvent.Type.HOLD_DONE, clockTime + delay, src);
        future.addEvent(evt);
        src.setState(SimEntity.State.HOLDING);
    }

    @Override
    public long getNumberOfFutureEvents(final Predicate<SimEvent> predicate) {
        return future.stream()
                .filter(predicate)
                .count();
    }

    @Override
    public void abort() {
    }

    /**
     * Prints a message about the progress of the simulation.
     *
     * @param message the message
     */
    private void printMessage(final String message) {
        Log.printLine(message);
    }

    @Override
    public boolean isPaused() {
        return paused;
    }

    @Override
    public final Simulation addOnSimulationPausedListener(final EventListener<EventInfo> listener) {
        Objects.requireNonNull(listener);
        this.onSimulationPausedListeners.add(listener);
        return this;
    }

    @Override
    public boolean removeOnSimulationPausedListener(final EventListener<EventInfo> listener) {
        return this.onSimulationPausedListeners.remove(listener);
    }

    @Override
    public final Simulation addOnEventProcessingListener(final EventListener<SimEvent> listener) {
        Objects.requireNonNull(listener);
        this.onEventProcessingListeners.add(listener);
        return this;
    }

    @Override
    public boolean removeOnEventProcessingListener(final EventListener<SimEvent> listener) {
        return onEventProcessingListeners.remove(listener);
    }

    @Override
    public Simulation addOnClockTickListener(final EventListener<EventInfo> listener) {
        Objects.requireNonNull(listener);
        onClockTickListeners.add(listener);
        return this;
    }

    @Override
    public boolean removeOnClockTickListener(final EventListener<? extends EventInfo> listener) {
        Objects.requireNonNull(listener);
        return onClockTickListeners.remove(listener);
    }

    @Override
    public NetworkTopology getNetworkTopology() {
        return networkTopology;
    }

    @Override
    public void setNetworkTopology(final NetworkTopology networkTopology) {
        this.networkTopology = networkTopology;
    }

    public void runStep(final double until) {
        if (running) {
            printMessage("Running simulation until: " + until + " starting clock: " + this.clock());

            while (running) {
                executeRunnableEntities();

                if (future.isEmpty()) {
                    running = false;
                    printMessage("Simulation: No more future events");
                } else {
                    // If there are more future events, then deal with them
                    Optional<SimEvent> maybeFirstEvent = eventWithinTimelimit(until);

                    if (maybeFirstEvent.isPresent()) {
                        maybeFirstEvent.ifPresent(this::processAllFutureEventsHappeningAtSameTimeOfTheFirstOne);
                    } else {
                        break;
                    }
                }
            }

            printMessage("Simulation until: " + until + " finished, clock: " + this.clock() + ", overall running: " + isRunning());
            printMessage("Correcting clock to: " + until);
            setClock(until);
        } else {
            printMessage("Simulation finished at " + this.clock());
        }
    }

    private Optional<SimEvent> eventWithinTimelimit(double until) {
        long start = System.currentTimeMillis();
        Optional<SimEvent> first = future.stream().filter(new Predicate<SimEvent>() {
            @Override
            public boolean test(SimEvent simEvent) {
                return simEvent.getTime() <= until;
            }
        }).findFirst();
        long stop = System.currentTimeMillis();
        //System.out.println(String.format(">>>>>>>>>>> STATS eventWithinTimelimit: %s %s", (stop-start)/1000.0, future.size()));
        return first;
    }

}
