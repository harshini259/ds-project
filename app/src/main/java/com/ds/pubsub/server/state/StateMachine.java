package com.ds.pubsub.server.state;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import com.ds.pubsub.proto.State;

public class StateMachine {

  private final Map<State, List<Consumer>> onStateHandlers = new HashMap<>();
  private final Map<State, List<Consumer>> onLeaveHandlers = new HashMap<>();

  private final Map<State, List<State>> transitions;
  private final AtomicReference<State> currentState;

  public State currentState() {
    return currentState.get();
  }

  public static final class Builder {
    private final Map<State, List<State>> transitions = new HashMap<>();
    public Builder addTransition(State from, State to) {
      if (transitions.containsKey(from)) {
        transitions.get(from).add(to);
      } else {
        transitions.putIfAbsent(from, new ArrayList<>());
        transitions.get(from).add(to);
      }
      return this;
    }

    private State initState;

    public Builder init(State state) {
      this.initState = state;
      return this;
    }

    public StateMachine build() {
      return new StateMachine(initState, transitions);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private StateMachine(State init, Map<State, List<State>> transitions) {
    this.transitions = Collections.unmodifiableMap(transitions);
    this.currentState = new AtomicReference<>(init);
  }

  public CompletableFuture<State> transition(State newState, Object obj) {
    CompletableFuture<State> futureState = new CompletableFuture<>();

    if (!currentState.get().equals(newState)) {
      CompletableFuture<State> leaveStateFuture = new CompletableFuture<>();

      State leaveState = currentState.get();

      if (allowed().contains(newState)) {
        CompletableFuture.supplyAsync(
            () -> {
              if (onLeaveHandlers.get(leaveState) != null) {
                System.out.println(onLeaveHandlers.get(leaveState));
                onLeaveHandlers.get(leaveState).forEach(action -> action.accept(obj));
              }
              leaveStateFuture.complete(leaveState);
              return leaveState;
            });

        leaveStateFuture.whenComplete(
            (success, error) -> {
              CompletableFuture.supplyAsync(
                  () -> {
                    this.currentState.set(newState);
                    if (onStateHandlers.get(newState) != null) {
                      onStateHandlers.get(newState).forEach(action -> action.accept(obj));
                    }
                    return currentState.get();
                  });
            });

      } else {
        futureState.completeExceptionally(
            new IllegalStateException(
                "not allowed transition from: " + currentState.get() + " to: " + newState));
      }
    }
    return futureState;
  }

  public List<State> allowed() {
    if (transitions.containsKey(currentState.get())) {
      return Collections.unmodifiableList(transitions.get(currentState.get()));
    }
    return List.of();
  }

  public void beforeExit(State state, Consumer consumer) {
    if (onLeaveHandlers.containsKey(state)) {
      onLeaveHandlers.get(state).add(consumer);
    } else {
      onLeaveHandlers.put(state, new ArrayList());
      onLeaveHandlers.get(state).add(consumer);
    }
  }

  public StateMachine on(State state, Consumer consumer) {
    if (onStateHandlers.containsKey(state)) {
      onStateHandlers.get(state).add(consumer);
    } else {
      onStateHandlers.put(state, new ArrayList());
      onStateHandlers.get(state).add(consumer);
    }

    return this;
  }
}
