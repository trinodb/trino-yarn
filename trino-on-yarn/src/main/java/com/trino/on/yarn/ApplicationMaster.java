/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trino.on.yarn;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.thread.GlobalThreadPool;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.trino.on.yarn.constant.Constants;
import com.trino.on.yarn.constant.RunType;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.executor.TrinoExecutorMaster;
import com.trino.on.yarn.server.MasterServer;
import com.trino.on.yarn.server.Server;
import com.trino.on.yarn.util.Log4jPropertyHelper;
import com.trino.on.yarn.util.OSUtil;
import com.trino.on.yarn.util.ProcessUtil;
import com.trino.on.yarn.util.YarnHelper;
import lombok.Data;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.trino.on.yarn.constant.Constants.HDFS;
import static com.trino.on.yarn.constant.Constants.S_3_A;

@Data
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final String INSUFFICIENT_MEMORY = "ip: {},insufficient memory, Need to memory: {}MB, The remaining memory: {}MB, waiting......";
    public static final int DEFAULT_APP_MASTER_TRACKING_URL_PORT = 8090;
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
    private static int amMemory = 128;
    private static JobInfo jobInfo = null;
    private static SimpleServer simpleServer = null;
    private static Process exec = null;

    static {
        RuntimeUtil.addShutdownHook(new Thread(() -> {
            ProcessUtil.killPid(exec, jobInfo);
            GlobalThreadPool.shutdown(true);
        }));
    }

    private final String appNodeMainClass;
    // Application Attempt Id ( combination of attemptId and fail count )
    @VisibleForTesting
    protected ApplicationAttemptId appAttemptID;
    // App Master configuration
    // No. of containers to run shell command on
    @VisibleForTesting
    protected int numTotalContainers = 1;
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    @VisibleForTesting
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    @VisibleForTesting
    protected AtomicInteger numRequestedContainers = new AtomicInteger();
    // Configuration
    private Configuration conf;
    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;
    // In both secure and non-secure modes, this points to the job-submitter.
    private UserGroupInformation appSubmitterUgi;
    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 10;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;
    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();

    // private YarnAppMasterHttpServer httpServer;
    // Args to be passed to the shell command
    private String shellArgs = "";
    private String javaOpts = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<>();
    private volatile boolean done;
    private ByteBuffer allTokens;
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<>();
    private ConcurrentHashMap<ContainerId, Container> runningContainers = new ConcurrentHashMap<>();
    // Container memory overhead in MB
    private int memoryOverhead = 10;

    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
        appNodeMainClass = ApplicationNode.class.getName();
    }

    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            exec = new TrinoExecutorMaster(jobInfo, amMemory).run();
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            while (Server.MASTER_FINISH.equals(0)) {
                ThreadUtil.sleep(500);
            }
            if (Server.MASTER_FINISH.equals(1)) {
                result = appMaster.finish();
            }
            LOG.info("ApplicationMaster finish");
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        } finally {
            ProcessUtil.killPid(exec, jobInfo);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private void dumpOutDebugInfo() {
        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        BufferedReader buf = null;
        try {
            String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir")
                    : Shell.execCommand("ls", "-al");
            buf = new BufferedReader(new StringReader(lines));
            String line;
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.cleanup(LOG, buf);
        }
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("master_memory", true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("memory_overhead", true,
                "Amount of memory overhead in MB for container");
        opts.addOption("java_opts", true, "Java opts for container");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");
        opts.addOption("job_info", true, "******json*******");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        // Check whether customer log4j.properties file exists
        if (fileExist(Constants.LOG_4_J_PATH)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, Constants.LOG_4_J_PATH);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "128"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }

        String jobInfoStr = Base64.decodeStr(cliParser.getOptionValue("job_info"));
        jobInfo = JSONUtil.toBean(jobInfoStr, JobInfo.class);
        LOG.warn("jobInfo:" + jobInfo);
        if (StrUtil.isNotBlank(jobInfo.getUser())) {
            System.setProperty("HADOOP_USER_NAME", jobInfo.getUser());
        }

        simpleServer = MasterServer.initMaster();
        jobInfo.setIpMaster(Server.ip());
        jobInfo.setPortMaster(simpleServer.getAddress().getPort());
        jobInfo.setPortTrino(NetUtil.getUsableLocalPort());
        jobInfo.setAmMemory(amMemory);
        jobInfo.setHeartbeat("http://" + jobInfo.getIpMaster() + ":" + jobInfo.getPortMaster() + Server.MASTER_HEARTBEAT);
        String absolutePath = new File(".").getAbsolutePath();
        jobInfo.setAppId(YarnHelper.getYarnAppId(absolutePath));
        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (fileExist(Constants.SHELL_ARGS_PATH)) {
            shellArgs = readContent(Constants.SHELL_ARGS_PATH);
        }

        if (fileExist(Constants.JAVA_OPTS_PATH)) {
            javaOpts = readContent(Constants.JAVA_OPTS_PATH);
        }

        if (cliParser.hasOption("shell_env")) {
            String[] shellEnvs = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "128"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        memoryOverhead = Integer.parseInt(cliParser.getOptionValue("memory_overhead", "1"));

        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType())) {
            String clientLogApi = Server.formatUrl(Server.CLIENT_LOG, jobInfo.getIp(), jobInfo.getPort());
            int freePhysicalMemorySize = OSUtil.getAvailableMemorySize();
            int memory = (int) (amMemory * 1.2);
            while (freePhysicalMemorySize < memory) {
                String logStr = StrUtil.format(INSUFFICIENT_MEMORY, jobInfo.getIpMaster(), amMemory, freePhysicalMemorySize);
                LOG.info(logStr);
                HttpUtil.post(clientLogApi, logStr, 3000);
                ThreadUtil.sleep(5000);
                freePhysicalMemorySize = OSUtil.getAvailableMemorySize();
            }
        }
        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public void run() throws Throwable {
        LOG.info("Starting ApplicationMaster");

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        appMasterHostname = NetUtils.getHostname();

        // Setup local RPC Server to accept status requests directly from clients
        //  need to setup a protocol for client to be able to communicate to
        // the RPC server
        //  use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);

        // Setup local RPC Server to accept status requests directly from clients
        //  need to setup a protocol for client to be able to communicate to
        // the RPC server
        //  use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
        if (containerMemory + memoryOverhead > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + (containerMemory + memoryOverhead) + ", max="
                    + maxMem);
            containerMemory = maxMem - memoryOverhead;
        }

        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                    + maxVCores);
            containerVirtualCores = maxVCores;
        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        recoverExecutors(previousAMRunningContainers);

        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();

        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        requestKContainers(numTotalContainersToRequest);
    }

    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    @VisibleForTesting
    protected boolean finish() {
        // wait for completion.
        while (!done) {
            if (Server.MASTER_FINISH.equals(2)) {
                break;
            }
            ThreadUtil.sleep(1000);
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }

        amRMClient.stop();
        ProcessUtil.killPid(exec, jobInfo);

        return success;
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        // what is the range for priority? how to decide?
        Priority pri = Priority.newInstance(requestPriority);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(containerMemory + memoryOverhead,
                containerVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        LOG.info("Requested container ask: " + request);
        return request;
    }

    private String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(Files.newInputStream(Paths.get(filePath)));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    private void recoverExecutors(List<Container> previousAMRunningContainers) {
        for (Container container : previousAMRunningContainers) {
            runningContainers.putIfAbsent(container.getId(), container);
        }
    }

    private void requestKContainers(int askCount) {
        LOG.info("Request new containers count:" + askCount);
        for (int i = 0; i < askCount; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers);
    }

    private synchronized void askMoreContainersIfNecessary() {
        int askCount = numTotalContainers - runningContainers.size();
        if (askCount > 0) {
            LOG.info("Request more containers count:" + askCount);
            requestKContainers(askCount);
        } else {
            LOG.info("No more to ask for containers");
        }
    }

    public ConcurrentHashMap<ContainerId, Container> getRunningContainers() {
        return runningContainers;
    }

    private boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    /**
     * NMCallbackHandler
     */
    static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private final ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();
        private final ApplicationMaster applicationMaster;

        NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info("Succeeded to stop Container " + containerId);
            applicationMaster.runningContainers.remove(containerId);
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            LOG.info("Container Status: id=" + containerId + ", status=" + containerStatus);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            LOG.info("Succeeded to start Container " + containerId);
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.runningContainers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            applicationMaster.runningContainers.remove(containerId);
            containers.remove(containerId);
        }
    }

    /**
     * RMCallbackHandler
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());
                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();

                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
                }
                runningContainers.remove(containerStatus.getContainerId());
            }

            // ask for more containers if any failed
            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClient.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            // We are sleeping here because there might be multiple calls
            // and we want to keep the number of containers as expected.
            if (runningContainers.size() >= numTotalContainers) {
                return;
            }
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container"
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory="
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores="
                        + allocatedContainer.getResource().getVirtualCores());
                // + ", containerToken"
                // +allocatedContainer.getContainerToken().getIdentifier().toString());

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
            ProcessUtil.killPid(exec, jobInfo);
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            return (float) numCompletedContainers.get()
                    / numTotalContainers;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            ProcessUtil.killPid(exec, jobInfo);
            amRMClient.stop();
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;

        NMCallbackHandler containerListener;

        // Set the local resources
        Map<String, LocalResource> localResources = new HashMap<>(4);

        /**
         * @param lcontainer        Allocated container
         * @param containerListener Callback handler of the container
         */
        LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        @Override
        public void run() {
            LOG.info("Setting up container launch container for containerId="
                    + container.getId());
            List<String> commands;
            if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType()) && jobInfo.getNumTotalContainers() == 1) {
                commands = yarnPer();
            } else {
                commands = yarnSession();
            }
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, shellEnv, commands, null, allTokens.duplicate(), null);
            runningContainers.putIfAbsent(container.getId(), container);
            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }

        private List<String> yarnPer() {
            String command = System.getenv("JAVA_HOME") + "/bin/java -version";
            List<String> commands = new ArrayList<>();
            commands.add(command);
            return commands;
        }

        private List<String> yarnSession() {
            Map<String, String> currentEnvs = System.getenv();
            if (!currentEnvs.containsKey(Constants.JAR_FILE_PATH)) {
                throw new RuntimeException(Constants.JAR_FILE_PATH
                        + " not set in the environment.");
            }
            String frameworkPath = currentEnvs.get(Constants.JAR_FILE_PATH);

            shellEnv.put("CLASSPATH", YarnHelper.buildClassPathEnv(conf));

            try {
                YarnHelper.addFrameworkToDistributedCache(frameworkPath, localResources, conf);
            } catch (IOException e) {
                Throwables.propagate(e);
            }

            String catalog = jobInfo.getCatalogHdfs();
            if (jobInfo.isHdfsOrS3()) {
                try {
                    YarnHelper.addToLocalResources(conf, catalog, Constants.JAVA_TRINO_CATALOG_PATH, localResources);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            if (RunType.YARN_SESSION.getName().equalsIgnoreCase(jobInfo.getRunType())) {
                try {
                    String path = "{}/tmp/trino/{}/";
                    String appId = appAttemptID.getApplicationId().toString();
                    if (StrUtil.startWith(catalog, S_3_A)) {
                        String bucket = catalog.replace(S_3_A, "").split("/")[0];
                        path = StrUtil.format(path, S_3_A + bucket, appId);
                    } else {
                        path = StrUtil.format(path, conf.get("fs.defaultFS", HDFS), appId);
                    }

                    org.apache.hadoop.fs.FileSystem fs = YarnHelper.getFileSystem(conf, path);
                    Path dst = new Path(path);
                    if (fs.exists(dst)) {
                        fs.delete(dst, true);
                    }
                    fs.mkdirs(dst);
                    path = path + "/" + StrUtil.uuid() + ".json";
                    dst = new Path(path);
                    FSDataOutputStream out = fs.create(dst);
                    out.write(jobInfo.toString().getBytes(StandardCharsets.UTF_8));
                    out.close();
                } catch (IOException | URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }

            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<>(10);

            // Set java executable command
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
            // Set am memory size
            vargs.add("-Xms" + containerMemory + "m");
            vargs.add("-Xmx" + containerMemory + "m");
            vargs.add("-Duser.language=zh");
            vargs.add("-Dfile.encoding=utf-8");
            vargs.add(javaOpts);

            // Set tmp dir
            vargs.add("-Djava.io.tmpdir=$PWD/tmp");

            // Set log4j configuration file
            // vargs.add("-Dlog4j.configuration=" + Constants.NESTO_YARN_APPCONTAINER_LOG4J);

            // Set class name
            vargs.add(appNodeMainClass);

            // Set args for the shell command if any
            vargs.add(shellArgs);
            // Add log redirect params

            vargs.add("--job_info " + Base64.encode(jobInfo.toString()));

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final command
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            LOG.info("Shell command is: \n" + command);

            List<String> commands = new ArrayList<>();
            commands.add(command.toString());
            return commands;
        }
    }
}
