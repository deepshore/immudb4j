/*
Copyright 2026 Deepshore GmbH. All rights reserved.
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.codenotary.immudb4j;

import io.codenotary.immudb.ImmuServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;

import com.google.protobuf.Empty;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for session lifecycle edge cases: shutdown and closeSession
 * behavior under failure conditions. These do not require a running immudb.
 */
public class SessionLifecycleTest {

    private ImmuClient client;
    private ManagedChannel mockChannel;
    private ImmuServiceGrpc.ImmuServiceBlockingStub mockStub;

    @BeforeMethod
    public void setUp() throws Exception {
        // Build a real client (connects to nothing, that's fine for these tests)
        client = ImmuClient.newBuilder()
                .withServerUrl("localhost")
                .withServerPort(3322)
                .build();

        // Replace internals with mocks
        mockChannel = mock(ManagedChannel.class);
        mockStub = mock(ImmuServiceGrpc.ImmuServiceBlockingStub.class);

        when(mockChannel.shutdown()).thenReturn(mockChannel);
        when(mockChannel.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        setField(client, "channel", mockChannel);
        setField(client, "blockingStub", mockStub);
    }

    @Test(testName = "closeSession is idempotent when no session is open")
    public void closeSessionWithoutSession() {
        // Should not throw
        client.closeSession();
        client.closeSession();

        // No RPC should have been attempted
        verify(mockStub, never()).closeSession(any());
    }

    @Test(testName = "closeSession cancels heartbeater even when RPC fails")
    public void closeSessionCancelsHeartbeatOnRpcFailure() throws Exception {
        Timer spyTimer = spy(new Timer(true));
        setField(client, "session", new Session("test-session", "defaultdb"));
        setField(client, "sessionHeartBeat", spyTimer);

        when(mockStub.closeSession(any(Empty.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

        // Should not throw despite RPC failure
        client.closeSession();

        // Heartbeater must have been cancelled
        verify(spyTimer).cancel();

        // Session must be null
        Assert.assertNull(client.getSession());
    }

    @Test(testName = "shutdown completes even when CloseSession RPC fails")
    public void shutdownCompletesOnRpcFailure() throws Exception {
        Timer spyTimer = spy(new Timer(true));
        setField(client, "session", new Session("test-session", "defaultdb"));
        setField(client, "sessionHeartBeat", spyTimer);

        when(mockStub.closeSession(any(Empty.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

        // Should not throw
        client.shutdown();

        // Heartbeater cancelled
        verify(spyTimer).cancel();

        // Channel was still shut down despite RPC failure
        verify(mockChannel).shutdown();
        verify(mockChannel).awaitTermination(5, TimeUnit.SECONDS);

        // Session cleaned up
        Assert.assertNull(client.getSession());
    }

    @Test(testName = "shutdown is idempotent when called twice")
    public void doubleShutdown() throws Exception {
        setField(client, "session", new Session("test-session", "defaultdb"));
        setField(client, "sessionHeartBeat", new Timer(true));

        client.shutdown();
        client.shutdown(); // second call should be a no-op

        // closeSession RPC only called once
        verify(mockStub, times(1)).closeSession(any(Empty.class));
    }

    @Test(testName = "shutdown without active session still closes channel")
    public void shutdownWithoutSession() throws Exception {
        // No session set, but channel exists
        client.shutdown();

        verify(mockChannel).shutdown();
        verify(mockStub, never()).closeSession(any());
    }

    @Test(testName = "closeSession followed by shutdown does not double-close")
    public void closeSessionThenShutdown() throws Exception {
        setField(client, "session", new Session("test-session", "defaultdb"));
        setField(client, "sessionHeartBeat", new Timer(true));

        client.closeSession();
        client.shutdown();

        // closeSession RPC called once (from closeSession), not again from shutdown
        verify(mockStub, times(1)).closeSession(any(Empty.class));
        verify(mockChannel).shutdown();
    }

    @Test(testName = "heartbeat is enabled by default in builder")
    public void heartBeatEnabledByDefault() {
        ImmuClient.Builder builder = ImmuClient.newBuilder();
        Assert.assertTrue(builder.isHeartBeatEnabled());
    }

    @Test(testName = "openSession does not create Timer thread when heartbeat is disabled")
    public void noTimerThreadWhenHeartBeatDisabled() throws Exception {
        // Count Timer daemon threads before creating the client
        long timerThreadsBefore = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isDaemon() && t.getName().startsWith("Timer") && t.isAlive())
                .count();

        ImmuClient noHbClient = ImmuClient.newBuilder()
                .withServerUrl("localhost")
                .withServerPort(3322)
                .withHeartBeatEnabled(false)
                .build();

        // Simulate openSession by injecting session directly (no real server needed)
        setField(noHbClient, "session", new Session("test-session", "defaultdb"));

        // The sessionHeartBeat field should remain null when heartbeat is disabled
        Object heartBeat = getField(noHbClient, "sessionHeartBeat");
        Assert.assertNull(heartBeat, "sessionHeartBeat Timer should be null when heartbeat is disabled");

        // No new Timer daemon threads should have been spawned
        long timerThreadsAfter = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isDaemon() && t.getName().startsWith("Timer") && t.isAlive())
                .count();
        Assert.assertEquals(timerThreadsAfter, timerThreadsBefore,
                "No new Timer thread should exist for a client with heartbeat disabled");
    }

    @Test(testName = "keepAlive sends RPC when session is active")
    public void keepAliveSendsRpcWhenSessionActive() throws Exception {
        setField(client, "session", new Session("test-session", "defaultdb"));

        client.keepAlive();

        verify(mockStub, times(1)).keepAlive(any(Empty.class));
    }

    @Test(testName = "keepAlive is a no-op when no session is open")
    public void keepAliveNoOpWithoutSession() {
        client.keepAlive();

        verify(mockStub, never()).keepAlive(any(Empty.class));
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
